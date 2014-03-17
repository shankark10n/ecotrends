import bs4 as bs
import datetime as dt
import os
import re
import sys

def get_url_from_toi_page():
	articles = []
	files = os.listdir('.')
	for fl in files:
		date = dt.datetime.strptime(fl, '%Y-%m-%d')
		page = bs.BeautifulSoup(open(fl).read())
		daily = page.find_all('a', href=re.compile('articleshow'))
		for each in daily:
			articles.append((date.strftime('%Y-%m-%d'), each.attrs['href'], 
				each.text.strip()))

# pattern for finding capitalised entities
capitalised_pat = '([A-Z][a-z]+ )([A-Z][a-z]+ |for|of|the)*([A-Z][a-z]+)'

def get_articles_from_mongo(tag = False):
	import pymongo as pm
	c = pm.Connection()
	db = c['mtw']
	coll = db['toi-drought-articles']

	i = 0
	wf = open('/home/shankar/work/nyu/food/data/drought.articles.tsv', 'w')
	for item in coll.find():
		if (i < 500):
			if tag:
				article = ' '.join([s.encode('utf-8') for s in \
													item['tagged-paragraphs']])
			else:
				article = ' '.join([s.encode('utf-8') for s in \
													item['paragraphs']])
			wf.write(item['date'].encode('utf-8') + '\t' + article + '\n')
			i += 1
		else:
			break
	wf.close()

def convert_csv_to_json(fname, district, state):
	'''Converts IMD rainfall data stored in CSV to a single JSON object.
	Expects district and state as inputs in addition to the filename of the 
	CSV file.
	'''
	import json
	entries = [l.strip().split(',') for l in open(fname).readlines()]
	json_obj = {}
	json_obj['district'] = district
	json_obj['state'] = state
	json_obj['annual_rainfall'] = []
	# auxiliary functions needed to parse rainfall and deviation in one line
	def odd(l): return [k for k in l if l.index(k) % 2 == 1]
	def even(l): return [k for k in l if l.index(k) % 2 == 0]
	for entry in entries:
		if entry is not None:
			for (month, rain, deviation) in zip(range(1,13),\
										 [rain for rain in even(entry[1:])],\
										 [dev for dev in odd(entry[1:])]):
				json_obj['annual_rainfall'].append({'month': month,\
													'rain': rain,\
													'deviation': deviation,\
													'year': entry[0]})
	print json.dumps(json_obj)

def classify_drought(district_json_obj, min_deviation = -25, min_months = 2,\
					 season = (2, 10)):
	'''Takes a json object and determines what mm/yy points can be classified
	as metereological drought as per IMD definition (-25 deviation: drought,
	-50: moderate, -75: severe). If deviation < -25 for 2 months, classified
	as drought. By default, only months during the season are considered for
	drought classification.

	Returns a modified JSON object with a new field for each object in the JSON
	array annual_rainfall containing drought classification.
	'''
	import json
	from itertools import chain
	if ((season[0] not in range(12)) | (season[1] not in range(12))):
		print 'season tuple must be in 1...12'
		return ''
	obj = json.loads(district_json_obj)
	period = len(obj['annual_rainfall'])
	# set is_drought to 0 by default for all
	for i in range(period):
		obj['annual_rainfall'][i]['is_drought'] = 0
	# evaluate is_drought for seasonal period
	for i in chain.from_iterable([range(period)[j+season[0]:j+season[1]+1]\
									for j in range(0, period, 12)]):
		try:
			avg_deviation = float(sum([float(item['deviation']) for item in \
								obj['annual_rainfall'][i-(min_months-1):i+1]]))/\
								min_months
			if (avg_deviation <= min_deviation):
				obj['annual_rainfall'][i]['is_drought'] = 1
		except ValueError:
			# deviation may be empty
			pass
		else:
			pass
		finally:
			pass
	print json.dumps(obj)

test_district_json_obj = '''{"state": "gujarat", "annual_rainfall": [{"deviation": "-58", "year": "2007", "rain": "0.2", "month": 1}, {"deviation": "-100", "year": "2007", "rain": "0.0", "month": 2}, {"deviation": "-100", "year": "2007", "rain": "0.0", "month": 3}, {"deviation": "-100", "year": "2007", "rain": "0.0", "month": 4}, {"deviation": "-100", "year": "2007", "rain": "0.0", "month": 5}, {"deviation": "-15", "year": "2007", "rain": "112.8", "month": 6}, {"deviation": "59", "year": "2007", "rain": "544.1", "month": 7}, {"deviation": "-25", "year": "2007", "rain": "243.8", "month": 8}, {"deviation": "55", "year": "2007", "rain": "258.8", "month": 9}, {"deviation": "-100", "year": "2007", "rain": "0.0", "month": 10}, {"deviation": "-96", "year": "2007", "rain": "0.7", "month": 11}, {"deviation": "-100", "year": "2007", "rain": "0.0", "month": 12}, {"deviation": "-100", "year": "2008", "rain": "0.0", "month": 1}, {"deviation": "-100", "year": "2008", "rain": "0.0", "month": 2}, {"deviation": "-100", "year": "2008", "rain": "0.0", "month": 3}, {"deviation": "40", "year": "2008", "rain": "0.7", "month": 4}, {"deviation": "-100", "year": "2008", "rain": "0.0", "month": 5}, {"deviation": "-76", "year": "2008", "rain": "32.3", "month": 6}, {"deviation": "-23", "year": "2008", "rain": "264.3", "month": 7}, {"deviation": "9", "year": "2008", "rain": "353.2", "month": 8}, {"deviation": "15", "year": "2008", "rain": "192.1", "month": 9}, {"deviation": "-67", "year": "2008", "rain": "8.0", "month": 10}, {"deviation": "-97", "year": "2008", "rain": "0.5", "month": 11}, {"deviation": "-97", "year": "2008", "rain": "0.1", "month": 12}, {"deviation": "-100", "year": "2009", "rain": "0.0", "month": 1}, {"deviation": "-100", "year": "2009", "rain": "0.0", "month": 2}, {"deviation": "-100", "year": "2009", "rain": "0.0", "month": 3}, {"deviation": "-100", "year": "2009", "rain": "0.0", "month": 4}, {"deviation": "-100", "year": "2009", "rain": "0.0", "month": 5}, {"deviation": "-90", "year": "2009", "rain": "12.8", "month": 6}, {"deviation": "-10", "year": "2009", "rain": "308.1", "month": 7}, {"deviation": "-62", "year": "2009", "rain": "123.0", "month": 8}, {"deviation": "-76", "year": "2009", "rain": "40.3", "month": 9}, {"deviation": "17", "year": "2009", "rain": "28.8", "month": 10}, {"deviation": "-18", "year": "2009", "rain": "12.8", "month": 11}, {"deviation": "-86", "year": "2009", "rain": "0.4", "month": 12}, {"deviation": "-100", "year": "2010", "rain": "0.0", "month": 1}, {"deviation": "-100", "year": "2010", "rain": "0.0", "month": 2}, {"deviation": "-100", "year": "2010", "rain": "0.0", "month": 3}, {"deviation": "-100", "year": "2010", "rain": "0.0", "month": 4}, {"deviation": "-100", "year": "2010", "rain": "0.0", "month": 5}, {"deviation": "-64", "year": "2010", "rain": "48.0", "month": 6}, {"deviation": "-15", "year": "2010", "rain": "290.1", "month": 7}, {"deviation": "16", "year": "2010", "rain": "374.7", "month": 8}, {"deviation": "29", "year": "2010", "rain": "214.8", "month": 9}, {"deviation": "-98", "year": "2010", "rain": "0.6", "month": 10}, {"deviation": "40", "year": "2010", "rain": "21.9", "month": 11}, {"deviation": "-100", "year": "2010", "rain": "0.0", "month": 12}, {"deviation": "-100", "year": "2011", "rain": "0.0", "month": 1}, {"deviation": "-100", "year": "2011", "rain": "0.0", "month": 2}, {"deviation": "-100", "year": "2011", "rain": "0.0", "month": 3}, {"deviation": "-100", "year": "2011", "rain": "0.0", "month": 4}, {"deviation": "-100", "year": "2011", "rain": "0.0", "month": 5}, {"deviation": "-96", "year": "2011", "rain": "5.0", "month": 6}, {"deviation": "-24", "year": "2011", "rain": "253.4", "month": 7}, {"deviation": "59", "year": "2011", "rain": "487.6", "month": 8}, {"deviation": "-27", "year": "2011", "rain": "120.2", "month": 9}, {"deviation": "-100", "year": "2011", "rain": "0.0", "month": 10}, {"deviation": "-100", "year": "2011", "rain": "0.0", "month": 11}, {"deviation": "-100", "year": "2011", "rain": "0.0", "month": 12}], "district": "baroda"}'''
if __name__ == '__main__':
	#convert_csv_to_json(sys.argv[1], sys.argv[2], sys.argv[3])
	#classify_drought(test_district_json_obj)
	for line in [line.strip() for line in open(sys.argv[1]).readlines()]:
		classify_drought(line)