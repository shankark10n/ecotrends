import datetime
import pymongo
import corpusbuilder as cb
import textutils as tu

isDBinit = False
conn = None

def db_handle(DBName):
    """Given a database name, sets up a connection and returns the handle.

    :param DBName:
    :returns: db handle
    """
    global isDBinit
    global conn
    if not isDBinit:
        conn = pymongo.Connection()
        isDBinit = True
    return conn[DBName]

def insert_document(paper):
    dbh = db_handle('arxiv')
    if dbh.papers.find({'id':paper['id']}).count() == 0:
        dbh.papers.insert(paper)

def num_records(DBName, collectionName):
    dbh = db_handle(DBName)
    return dbh.collectionName.find().count()

def num_papers_in_db():
    dbh = db_handle('arxiv')
    return dbh.papers.find().count()

def clear_paper_db():
    dbh = db_handle('arxiv')
    dbh.papers.remove({})

def get_most_recent(fromdate = '2012-03-01', category = ''):
    '''
    Returns a list of papers from DB from fromdate for a given category. IF
    category is not mentioned, then returns papers from all categories. Note
    that fromdate is given in '%Y-%m-%d' format.
    '''
    query = {}
    if category != '':
        query['tags'] = category
    query['updated_date'] = {'$gt': datetime.datetime.strptime(fromdate, '%Y-%m-%d')}
    return [p for p in db_handle('arxiv').papers.find(query)]

def update_paper(key, value, paper_id = ''):
    '''
    Updates a paper in the DB. If paper_id is not specified, then all papers
    are updated with the given (key, value) pair.
    '''
    dbh = db_handle('arxiv')
    try:
        if paper_id is '':
            dbh.papers.update({}, {'$set': {key: value}}, safe = True, multi = True)
            print 'Successfully updated all documents'
        else:
            dbh.papers.update({'id': paper_id}, {'$set': {key: value}}, safe = True, multi = False)
            print 'Successfully updated document with ID:%s' % paper_id
    except KeyError as ke:
        print 'Error: key %s does not exist. Update failed.' % key
        print ke.message

def apply_transform(key, fn, paper_id = ''):
    '''
    Applies a transform to a field given by 'key' for a paper. If paper_id is not
    specified, then all papers are updated with the given (key, fn(key)) pair.
    '''
    dbh = db_handle('arxiv')
    try:
        if paper_id is '':
            allpapers = dict([(p['id'], p[key]) for p in dbh.papers.find({})])
            for i in allpapers:
                dbh.papers.update({'id': i}, {'$set': {key: fn(allpapers[i])}}, safe = True, multi = True)
            print 'Successfully updated all documents'
        else:
            currentvalue = dbh.papers.find_one({'id': paper_id})[key]
            dbh.papers.update({'id': paper_id}, {'$set': {key: fn(currentvalue)}}, safe = True, multi = False)
            print 'Successfully updated document with ID:%s' % paper_id
    except KeyError as ke:
        print 'Error: key %s does not exist. Update failed.' % key
        print ke.message

def refresh_paper_db(till = datetime.datetime.today()):
    # Get old state
    for category in utils.get_categories():
        lastrun = utils.get_log_date(category)
        papers = paper.get_papers(lastrun, till, category)
        for p in papers:
            score = 0
            datefmt = '%Y-%m-%dT%H:%M:%SZ'
            date = p['updated_date'] #datetime.datetime.strptime(p['updated_date'], datefmt)
            if (date < lastrun):
                continue
            print ' adding %s to DB' % p['id']
            insert_paper(p)
        utils.save_log_date(newcategory = category, newdate = till)
