import re
import os
import pymongo as pm
import sys
import bs4
import cPickle
import json
from collections import OrderedDict as od

def init_db(filename = '/scratch/sunandan/state_month_wbkw_all.p'):
	c = pm.Connection()
	db = c.mtw
	dic = cPickle.load(open(filename))
	return db.et_data, dic

def wb_extract(state, format='json'):
	et, dic = init_db()
	st = dict([(k, dic[k]) for k in dic if k.startswith(state)])
	pat = '%s-(20[0-9]{2}-[0-9]{2})' % state
	l = []
	keys = st.keys()
	keys.sort()
	item_keys = ['date', 'state', 'title', 'wb_keywords', 'url', 'text']
	for item in keys:
		mmyy = re.findall(pat, item)[0]
		articles = [i for i in et.find({'state': state, 'date': re.compile(mmyy)})]
		indicators = set(st[item])
		for article in articles:
			text = ' '.join(article['paragraphs'])
			text_indicators = [i for i in indicators if i in text.lower()]
			obj = {'date': article['date'], 'state': article['state'],\
			 'url': article['url'], 'wb_keywords': ';'.join(text_indicators),\
			  'title': article['title'], 'text': text}
			l.append(obj)
			# oof.write('%s\n' % '{%s}' % ', '.join(['%s:%s' %\
			#  (key, obj[key]) for key in obj]).encode('utf8'))
	if format is 'json':
		oof = open('/scratch/shankar/wbindicators/%s.json' % state, 'w')
		oof.write('\n'.join([str(item).encode('utf8') for item in l]))
	elif format is 'tsv':
		oof = open('/scratch/shankar/wbindicators/%s.tsv' % state, 'w')
		oof.write('\t'.join(item_keys)+'\n')
		oof.write('\n'.join(['\t'.join([item[e] for e in item_keys]).encode('utf8') for item in l]))
	oof.close()

if __name__ == '__main__':
	states = ['chandigarh', 'andhra pradesh', 'karnataka', 'westbengal', 'uttarpradesh', 'kerala', 'tamilnadu', 'gujarat']
	for state in states:
		wb_extract(state, format='tsv')