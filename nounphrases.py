import re
import nltk
import os
import sys
from nltk.corpus import stopwords
from itertools import chain
import datetime as dt

cm=lambda x: ','.join(x)

def build_nounphrases(filenames = [], exclude_unigrams = False, top_threshold = 2):
	'''Returns an NLTK freqdist dictionary containing all nounphrases collected
	 from titles of articles. Also returns a list of top phrases extracted from
	 the dictionary for ready reference.
	 @param filenames
	 @param exclude_unigrams Should unigrams be excluded from top dictionary
	 @param top_threshold min. value in dictionary for key to be included in top.
	'''
	# ignores NPs of the form Mr. India
	npat='\(N+P?S? ([A-Za-z ]+)\)'
	nre=re.compile(npat)
	nps = []
	for fn in fns:
		try:
			# line is tab-separated, with parse tree of NP in col 3
 			nps.extend(list(chain.from_iterable([[' '.join(nre.findall(w))\
 			 for w in l.strip().split('\t')[2].split(',')\
 			  if len(nre.findall(w))] for l in open(fn).readlines()])))
 		except IndexError, e:
 			# some rows have no parse tree, so do nothing
 			print 'Ignoring missing parse tree error.'
 			pass
 		except IOError, e:
 			print 'Ignoring missing file error.', e.message
 			pass
 	npfd = nltk.FreqDist([w.lower() for w in nps])
 	topnps = [k for k in npfd if npfd[k] >= top_threshold]
 	if exclude_unigrams:
 		topnps = [k for k in topnps if re.findall(' ', k)]
 	return (npfd, topnps)