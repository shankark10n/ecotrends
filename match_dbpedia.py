import os
import pymongo as pm
import re
import sys

def extract_entities(article, stopwords = ['A', 'The', 'In', 'At']):
	'''Takes article text and matches entities from DBPedia.
	@param article Dictionary containing article text and other metadata
	@param dbp DBPedia handle in mongodb
	@returns list of entities in article text
	'''
	# ngrams = lambda l, k: [' '.join(l[i:i+k]) for i in xrange(len(l)-k+1)]
	article_entities = []
	
	# patterns
	camel_pat = '[A-Z][a-z]+'
	ngram_pat = lambda n: ' '.join([camel_pat for i in xrange(n)])

	# lower-case article text
	article_text = ' '.join(article['paragraphs'])
	article_entities.extend(re.findall(ngram_pat(2), article_text))
	article_entities.extend(re.findall(ngram_pat(3), article_text))
	article_entities.extend(re.findall(ngram_pat(4), article_text))
	article_entities = filter_entities(article_entities, stopwords)
	return set(article_entities)

def update_entitydb(entity_coll, article):
	'''Updates the entity db with entities from the article.
	@param entity_coll mongodb collection of entities
	@param article article
	'''
	entities = extract_entities(article)
	# article metadata common for all entities
	entity_keys = article.keys()
	entity_keys.remove('paragraphs')
	entity_keys.remove('_id')
	parent_article = {}
	for k in entity_keys:
		parent_article[k] = article[k]
	for e in entities:
		ee = entity_coll.find({'entity': e})
		if not ee.count(): 
			# entity not found in collection; so must insert
			entity_obj = {}
			entity_obj['entity'] = e
			entity_obj['article_list'] = []
			entity_obj['article_list'].append(parent_article)
			entity_coll.insert(entity_obj)
		else:
			# entity already present; insert parent_article in sorted list of
			# articles
			entity_obj = [eo for eo in ee][0] # only expect to find single object per entity
			parent_articles = entity_obj['article_list']
			parent_articles.append(parent_article)
			entity_coll.update({'entity': e}, {"$set": {'article_list': parent_articles}})

def test_update_entitydb(db, articles):
	entity_coll = db['test_entitydb']
	i = 0
	for article in articles:
		i += 1
		print 'Updating db with article #%d: %s' % (i, article['title'].encode('utf8'))
		update_entitydb(entity_coll, article)
	pass

def filter_entities(entity_list, stopwords):
	entities = [e for e in entity_list if e.split(' ')[0] not in stopwords]
	return entities