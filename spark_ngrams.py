from pyspark import SparkContext, SparkConf
from operator import add
import json
import random
import numpy as np

def concatenate_paragraphs(sentence_array):
	return ' '.join(sentence_array).split(' ')

stopwords = set(['a', 'an', 'of', 'the', 'by', 'at', 'or', 'in', 'to', 'up', 'into', 'until', 'before', 'but', 'on', 'is', 'for', 'and', 'as', 'with', 'while', 'when', 'where', 'why', 'has', 'had', 'have', 'that', 'there', 'then'])
logDir = '/home/sunandan/et-2006/'
conf = SparkConf()
conf.setMaster("spark://beaker-16:7077").setAppName("example").set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)
logData = sc.textFile(logDir) # reads all files in the folder
num_records = logData.count() # counts number of records
print 'Number of lines: %d' % num_records
tm = logData.map(lambda s: [(json.loads(s)['date'], word.lower()) for word in json.loads(s)['text'].split(' ')])
tm = tm.flatMap(lambda l: l).filter(lambda l: l[1] not in stopwords).map(lambda l: (l, 1))
# tm = tm.flatMap(lambda l: l).map(lambda l: (l, 1))
tm = tm.reduceByKey(lambda _, x: _ + x).map(lambda x: (x[1], x[0])).sortByKey()
# tm = tm.flatMap(lambda l: l).countByKey().map(lambda x: (x[1], x[0][0], x[0][1])).sortByKey(ascending=False)
op = tm.collect()
# for word, date, count in op:
# 	print 'word: %s, date: %s, count: %d' % (word, date, count)
# for count, date, word in op:
# 	print 'date: %s, word: %s, count: %d' % (date, word, count)
for x in op:
	print x