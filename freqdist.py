import re
import sys
import cPickle
from nltk import FreqDist
import datetime as dt

boul = lambda x: 1 if x else 0
tcmp = lambda x,y: -1 if x[1] < y[1] else boul(y[1] < x[1])
strtodate = lambda date: dt.datetime.strptime(date, '%Y%m%d')
datetostr = lambda date: dt.datetime.strftime(date, '%Y%m%d')

def find_most_frequent_terms(dd, nterms = 5, date = '20120101', blacklist = []):
	'''Given a freqdist dd, retrieves nterms most frequent
	entities with frequency for a given date.
	'''
	terms = []
	blre = [re.compile(pat) for pat in blacklist]
	if dd.has_key(date):
		items = [item for item in dd[date].items()\
				 if all([not(len(r.findall(' '.join(item[0]))))\
				  		for r in blre])]
		items.sort(tcmp)
		return items[-nterms:]
	else:
		#print 'date:%s not found' % date
		pass

def get_inverted_index(filename, date = '20120601', pre=0, post=0, blacklist=[]):
	'''Returns an inverted index of terms occurring before/after a given number 
	of days.

	Returns a dictionary with keys as terms and values as lists of dates term
	occurred in.
	'''
	from collections import defaultdict as dd
	ii = dd(set)
	cp = cPickle.load(open(filename))
	start = strtodate(date)
	styy = date[:4] + '0101'
	enyy = date[:4] + '1231'
	days = [start - dt.timedelta(i) for i in range(pre)\
				 if (start - dt.timedelta(i)) >= strtodate(styy)]
	days.extend([start + dt.timedelta(i+1) for i in range(post)\
				if (start + dt.timedelta(i)) <= strtodate(enyy)])
	days.sort()
	for day in days:
		# get all terms for the day
		termfreq = find_most_frequent_terms(dd=cp, date=datetostr(day),\
											blacklist=blacklist, nterms=0)
		if termfreq:
			terms = [' '.join(i[0]) if isinstance(i[0], tuple) else i[0]\
				 		for i in termfreq]
		if terms:
			for term in terms:
				# add (t-start) to set for each term occurring at time=t
				ii[term].add((day - start).days)
	return ii

def timeseries_most_frequent_terms(filename, nterms = 10, date = '20120101', pre = 0, post = 0, blacklist = []):
	'''Given a freqdist stored as a cPickle file, retrieves a timeseries with
	most frquent terms for pre days before and post days after date.
	'''
	dd = cPickle.load(open(filename))
	ts = []
	start = strtodate(date)
	styy = date[:4] + '0101'
	enyy = date[:4] + '1231'
	days = [start - dt.timedelta(i) for i in range(pre)\
				 if (start - dt.timedelta(i)) >= strtodate(styy)]
	days.extend([start + dt.timedelta(i+1) for i in range(post)\
				if (start + dt.timedelta(i)) <= strtodate(enyy)])
	days.sort()
	for day in days:
		terms = find_most_frequent_terms(dd=dd, date=datetostr(day),\
					blacklist = blacklist, nterms=nterms)
		# print '%s:%d' % (datetostr(day), len(terms))
		if terms:
			ts.append((datetostr(day), terms))
	return ts

def timeseries_most_frequent_terms_wrapper(args, blacklist):
	if len(args) < 3:
		print 'Usage: %s <freq-dist file> <date in YYYYmmdd format>. By default assumes date=20120525\n'\
		 % args[0]
		ts = timeseries_most_frequent_terms(args[1], date='20120525', pre=30,\
				 post=30, blacklist=blacklist)
	else:
		ts = timeseries_most_frequent_terms(args[1], date=args[2], pre=30,\
				 post=30, blacklist=blacklist)
	print '\n'.join(['%s: {%s}' % (day[0], ','.join(['(%s:%d)'\
											 % (' '.join(item[0])\
											  if isinstance(item[0], tuple)\
											   else item[0], item[1])\
											 	for item in day[1]]))\
					for day in ts])

def get_inverted_index_wrapper(args, blacklist):
	if len(args) < 3:
		print 'Usage: %s <freq-dist file> <date in YYYYmmdd format>. By default assumes date=20120601\n'\
		 % args[0]
		inv_index = get_inverted_index(filename=args[1], date='20120601', pre=90,\
			post=0, blacklist=blacklist)
	else:
		inv_index = get_inverted_index(filename=args[1], date=args[2], pre=90,\
			post=0, blacklist=blacklist)
	lcmp = lambda x, y: -1 if len(x[1]) < len(y[1]) else boul(len(x[1]) > len(y[1]))
	items = inv_index.items()
	items.sort(lcmp)
	if not(len(items)):
		print 'Empty list.'
	for item in items:
		dates = list(item[1])
		dates.sort()
		print '"%s",{%s}' % (item[0], ','.join([str(i)\
														  for i in dates]))
	# print 'total count of terms: %d' % len(items)
if __name__ == '__main__':
	blacklist = ['euro', 'bolly', 'ipl', 'holly', 'epl', 'cricket']
	# timeseries_most_frequent_terms_wrapper(sys.argv, blacklist)
	get_inverted_index_wrapper(sys.argv, blacklist)