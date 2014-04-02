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

def lag_week(week, d):
	'''Given a week number week, return (week-d>=0 ? (week-d) : -1)
	'''
	if ((week-d) >= 0):
		return (week-d)
	else:
		return -1

D = 3

conf = SparkConf()
conf.setMaster("spark://beaker-2:7077").setAppName("example").set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)
txt = sc.textFile('2006nps.json').cache() # reads all files in the folder
num_records = txt.count() # counts number of records; enforce staying in cache

tm = txt.map(lambda l: (int(json.loads(l)['date']), json.loads(l)['text']))

# create a sorted vector of distinct ngrams
ngrams = tm.flatMap(lambda x: x[1]).distinct()
# create a history of upto D weeks
history = sc.parallelize(xrange(D))
# now return a sorted cartesian product (e.g. ('a', 0), ('a', 1), ('a', 2), ('b', 0), ('b', 1), ...)
ngrams_history = ngrams.cartesian(history).sortByKey()

# create a dict of RDDs keyed on successively lagging weeks
tm_dict = {}
for x in xrange(1, D+1):
	rdd = tm.map(lambda tple: (lag_week(tple[0], x), tple[1]))
	tm_dict[x] = rdd

# right-outer join all the RDDs to retain the -1s from successive rdds
tm = txt.map(lambda s: [(year_of_date(json.loads(s)['date']),\
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