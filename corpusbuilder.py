"""Given a doc, extracts entities, frequencies, and stores in a corpus.
"""

from collections import defaultdict as dd
import jsonpickle
import entityxctor as ex
import textutils as tu

class Entity:
    def __init__(self, *args, **kwargs):
        """Constructor for the entity class. Contains at minimum, an NER tag
        (per, loc, org, cat)

        :param value: name of entity
        :param id: id of document that contains entity
        :param tag: type of entity obtained from a tagger, presumably
        """
        self.__id = ''
        self.__tag = ''
        if 'docid' in kwargs:
            self.__id = kwargs['docid']
        if 'tag' in kwargs:
            self.__tag = kwargs['tag']
        if 'value' not in kwargs:
            raise KeyError('Entity value required')
        self.__value = kwargs['value']

class Context:
    def __init__(self, *args, **kwargs):
        """Constructor for the context class. A context object tells
        where an entity occurred. Contains sentence, numbers associated,
        placeholders for trends.

        :param sentence: text of sentence containing entity
        :param numbers: list of numerical values
        """
        self.sentence = ''
        self.numbers = []
        if 'sentence' not in kwargs:
            raise KeyError('Sentence argument is needed for instantiating \
context for entity')
        self.sentence = kwargs['sentence']
        if 'numbers' in kwargs:
            self.numbers = kwargs['numbers']

    def __str__(self):
        return '{sentence:\"' + self.sentence +\
               '\",numbers:' + '[' + ','.join(self.numbers)\
               + ']}'
              

class Document:
    def __init__(self, *args, **kwargs):
        """Constructor for the document object. Takes doc ID, text of doc
        and constructs object containing entities and frequency count for
        each entity.
        """
        if 'docid' not in kwargs:
            raise KeyError('Document id required.')
        if 'doctext' not in kwargs:
            raise KeyError('Document text required.')
        self.__id = kwargs['docid']
        self.__text = kwargs['doctext']
        self.__entities = ex.wiki_ngram_xctor(self.__text,\
                                              path = '/home/shankar/tmp/wiki/enwiki-latest-all-titles-in-ns0-part')
        self.__occurrences = dd(list)
            
    def build_occurrences(self):
        """Builds occurrences for all entities in the document. An occurrence
        is thought of as just a list of Context objects for an entity.
        """
        # Collect sentences from text
        sentences = tu.get_sentences(self.__text)
        for entity in self.__entities:
            for sentence in sentences:
                ngrams = set(tu.get_ngrams(sentence))
                tokens = tu.tokenize_text(sentence)
                if entity in ngrams:
                    # Collect numbers in sentence
                    numbers = [token for token in tokens if tu.is_number(token)]
                    context = Context(sentence = sentence, numbers = numbers)
                    self.__occurrences[entity].append(context)

    def get_id(self):
        return self.__id

    def get_text(self):
        return self.__text

    def get_entities(self):
        return self.__entities

    def get_occurrences(self, entity = ''):
        """Given an entity, returns the list of all appearances of the entity 
        in the document. If no entity is given, returns entire list

        :param entity: value of entity to be checked (assumed lower-cased)
        :returns: list of Context objects for the entity.
        """
        if len(self.__occurrences) == 0:
            self.build_occurrences()
        if entity == '':
            return self.__occurrences
        else:
            return self.__occurrences[entity]

class Corpus:
    def __init__(self, *args, **kwargs):
        """Constructor for the corpus object. Takes a list of documents, and
        constructs the corpus.
        """
        self.__docids = set()
        self.__entities = dd(int)
        self.__entity_index = dd(list)

    def add_document(self, doc):
        """Adds a document to the corpus, by extracting entities, frequencies
        etc.

        :param doc: document object containing doc ID, doc text, ngram entities
        and frequencies
        """
        self.__docids.add(doc.get_id())
        for e in doc.get_entities():
            self.__entities[e] += doc.get_entities()[e]
            self.__entity_index[e].append(doc.get_id())

    def get_docids(self):
        return self.__docids

    def get_frequency(self, entity):
        """Returns #occurrences of an entity

        :param entity: entity (will be lower-cased)
        :returns: count #occurrences
        """
        return self.__entities[entity.strip().lower()]

    def get_entities(self):
        return self.__entities

    def __str__(self):
        pickled = jsonpickle.encode(self)
        return str(pickled)
