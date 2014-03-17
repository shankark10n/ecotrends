import urllib2
import re
import bs4
import datetime as dt
import time
import sys

strtodate = lambda date, format: dt.datetime.strptime(date, format)
datetostr = lambda date, format: dt.datetime.strftime(date, format)

def fetch_todays_paper(date = '2006-01-01', loghandle = ''):
	'''Fetches all content from the index page for a date's news from the Hindu
	using the format: http://www.thehindu.com/todays-paper/tp-index/?date=%Y-%m-%d

	Returns a list of tuples in the format: (date, url, title)
	'''
	page = urllib2.urlopen('http://www.thehindu.com/todays-paper/tp-index/?date=%s' %\
		date).read()
	soup = bs4.BeautifulSoup(page)
	pat = 'thehindu.+today.+ece$'
	articles = [(date, e['href'].encode('utf8'), e.text.encode('utf8'))\
			 for e in soup.find_all(href=re.compile(pat))]
	if (loghandle):
		loghandle.write('[%s]: fetched %d articles for %s\n' %\
		 (time.ctime(), len(articles), date))
	loghandle.flush()
	return articles

if __name__ == '__main__':
	yy = int(sys.argv[1])
	lf = open('hindu.%d.log.txt' % yy, 'w')
	start_date = strtodate('%d-01-01' % yy, '%Y-%m-%d')
	end_date = strtodate('%d-12-31' % yy, '%Y-%m-%d')
	days = (end_date - start_date).days
	of = open('hindu-%s.tsv' % str(yy), 'w')
	of.write('\n'.join(['\n'.join(['\t'.join(item)\
			 						for item in fetch_todays_paper(date, loghandle = lf)])\
			 			 for date in\
			 			  [datetostr(start_date + dt.timedelta(i), '%Y-%m-%d')\
			 			 	for i in range(days+1)]\
			 			]))
	of.close()
	lf.close()