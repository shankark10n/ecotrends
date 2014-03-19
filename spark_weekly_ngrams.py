from pyspark import SparkContext, SparkConf
from operator import add
import json
import random
import numpy as np
from datetime import datetime

def concatenate_paragraphs(sentence_array):
	return ' '.join(sentence_array).split(' ')

def year_of_date(date):
	return datetime.strptime(date, '%Y-%m-%d').year

def week_of_date(date):
	yr = year_of_date(date)
	start = datetime(yr, 1, 1)
	return  (datetime.strptime(date, '%Y-%m-%d')-start).days/7

stopwords = set(['a', 'an', 'of', 'the', 'by', 'at', 'or', 'in', 'to', 'up', 'into', 'until', 'before', 'but', 'on', 'is', 'for', 'and', 'as', 'with', 'while', 'when', 'where', 'why', 'has', 'had', 'have', 'that', 'there', 'then'])
logDir = '/home/sunandan/data/weekly_data/201*'
conf = SparkConf()
conf.setMaster("spark://beaker-16:7077").setAppName("example").set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)
logData = sc.textFile(logDir) # reads all files in the folder
num_records = logData.count() # counts number of records
print 'Number of lines: %d' % num_records
# flatMap will flatten even the first element of a tuple with a list in it 
# (e.g. [(a, [b, c]), ...] becomes [a, b, c, ...]). So fix this by sending
# [(a,b), (a, c), ...] which then becomes (a, b), (a, c), ...
tm = logData.map(lambda s: [(year_of_date(json.loads(s)['date']),\
							 week_of_date(json.loads(s)['date']),\
							  word.lower()) for word in json.loads(s)['text'].split(' ')])\
			.flatMap(lambda l: l).filter(lambda l: l[2] not in stopwords)\
			.map(lambda l: (l, 1))\
			.reduceByKey(lambda _, x: _ + x)\
			.map(lambda x: (x[1], x[0]))\
			.sortByKey(ascending=False)\
			.filter(lambda v: (v[0] > 5) and (v[0] <= 100))\
			.map(lambda v: (v[0], v[1][0], v[1][1], v[1][2]))
nr = tm.count()
print 'Num of records: %d' % nr
op = tm.take(1000)
# tm = tm.flatMap(lambda l: l).map(lambda l: (l, 1))
# for word, date, count in op:
# 	print 'word: %s, date: %s, count: %d' % (word, date, count)
# for count, date, word in op:
# 	print 'date: %s, word: %s, count: %d' % (date, word, count)
# for x in op:
# 	print 'date: %s, word: %s, count: %d' % (x[1][0], x[1][1], x[0])
for count, year, week, word in op:
	print 'year: %d, week: %d, word: %s, count: %d' % (year, week, word, count)
# for x in op:
# 	print x