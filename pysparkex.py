from pyspark import SparkContext, SparkConf
from operator import add
import json
import random
import numpy as np

def concatenate_paragraphs(sentence_array):
	return ' '.join(sentence_array).split(' ')

#logFile = "/home/shankar/space/spark/spark-0.9.0-incubating/README.md"  # Should be some file on your system
# logFile = "/home/shankar/toi_dump/raw.json"
# logFile = '/home/shankar/npfd.txt'
logFile = '/home/shankar/jul42006.json'
# sentimentFile = "/home/shankar/sentiment.txt"
conf = SparkConf()
conf.setMaster("spark://beaker-16:7077").setAppName("example").set("spark.executor.memory", "1g")
#sc = SparkContext("local", "Simple App")
sc = SparkContext(conf=conf)
logData = sc.textFile(logFile).cache()
num_lines = logData.count()
print 'Number of lines: %d' % num_lines
# month_key = sc.parallelize(np.random.randint(0, 12, num_lines), 4)
# sentData = sc.textFile(sentimentFile).cache()
#pos_sent = sentData.flatMap(lambda l: [i[0] for i in l.split(',') if i[1]==1)
#neg_sent = sentData.flatMap(lambda l: [i[0] for i in l.split(',') if i[1]==0)
# text_month = month_key.cartesian(logData)
tm = logData.map(lambda s: (json.loads(s)['state'], len(concatenate_paragraphs(json.loads(s)['paragraphs']))))
tm = tm.reduceByKey(lambda _, x: _ + x)
# text_month = text_month.map(lambda p: (p[0], len(p[1].split(' '))))
# text_month_aggregate = text_month.reduceByKey(add)
# op = text_month_aggregate.collect()
op = tm.collect()
for state, num_words in op:
	print 'state: %s, num_words: %d' % (state, num_words)
#text_month_aggregate_pos_neg = text_month_aggregate.map(lambda ss: len(set(ss.split( 
#text_month_pos_neg = logData.flatMap(lambda s: (random.randint(0,11), s['text'].split(' ').lower()))
#			.filter(lambda x: 

#numAs = logData.filter(lambda s: 'a' in s).count()
#numBs = logData.filter(lambda s: 'b' in s).count()

#print "Lines with a: %i, lines with b: %i" % (numAs, numBs)
