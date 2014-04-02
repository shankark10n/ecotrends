import nltk
import re
from pyspark import SparkContext, SparkConf
from operator import add
import json
import random
import numpy as np
import cPickle as cp
import datetime

tags = ['CD','JJ','JJS','JJR','NN','NNP','NNPS','NNS']

def getNPs(data):
    words = []
    for k in data:
        if k[1] in tags:
            words.append(k[0].lower())
        else:
            words.append('***')
    nps = []
    phrases = []
    for word in words:
        if word == '***':
            if len(nps)>0:
                phrases.append(' '.join(nps))
            nps = []
        else:
            nps.append(word)
    return phrases

def year_of_date(date):
    return date.year

def week_of_date(date):
    dt = date.split('-')   
    date = datetime.date(int(dt[0]),int(dt[1]),int(dt[2]))
    yr = year_of_date(date)
    start = datetime.date(yr, 1, 1)
    return  (date-start).days/7



dataDir = '/scratch/sunandan/data/newsinjson/et/2006/'
dataDir = '/home/sunandan/data/tmp'
dataDir = '/home/sunandan/tmpdata/2006-6-months/'

conf = SparkConf()
conf.setMaster("spark://beaker-6:7077").setAppName("pos_tagging").set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)
data = sc.textFile(dataDir) # reads all files in the folder

tm = data.map(lambda x: (json.loads(x)['date'],getNPs(nltk.pos_tag(nltk.word_tokenize(json.loads(x)['text'])))))\
    .reduceByKey(lambda x,y: x+y)\
    .map(lambda x: (week_of_date(x[0]),x[1]))\
    .reduceByKey(lambda x,y: x+y)\
    .sortByKey(ascending=True)
    
op = tm.collect()

print data.count()
total=0
uiq = 0
nps = {}
wr = open('/scratch/sunandan/2006nps.txt','w')
nplist = []
for date,tagged_sents in op:
    print date,list(set(tagged_sents))
    opdict = {}
    # wr.write(str(date)+' '+str(list(set(tagged_sents))))
    # wr.write('\n')
    opdict['date'] = str(date)
    opdict['text'] = (list(set(tagged_sents)))
    nplist.append(opdict)
    uiq = len(list(set(tagged_sents)))
    total = total+len(tagged_sents)
print total,uiq
#cp.dump(nps,open('/scratch/sunandan/2006nps.p','w'))

wr = open('/scratch/sunandan/2006nps_cleaner.json','w')
wr.write('\n'.join([json.dumps(i) for i in nplist]))
wr.close()

