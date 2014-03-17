import textutils as ut

def get_sample(urlfile, num = 50):
    """From a list of URLs in a file, retrieves a sample of URLs and gets
    the body of the article. Samples can be too big, so for now they are
    restricted to 'num'.
    """
    import Utilities as ut
    sampleurls = ut.get_sample_file(urlfile)
    sample = []
    for url in sampleurls[:num]:
        sample.append((url, ut.get_body(url)))
    return sample

def output_sample(urlfile, num = 50):
    """From a list of URLs in a file, retrieves a sample of URLs and gets
    the body of the article. Samples can be too big, so for now they are
    restricted to 'num'.
    """
    of = open('sample.txt', 'w')
    import Utilities as ut
    sampleurls = ut.get_sample_file(urlfile)
    count = num
    for (url, body) in get_sample(urlfile, num):
        of.write(url + '\n')
        of.write(body + '\n\n')

def build_dtm(filenames):
    """Given a list of text files, builds a document-term matrix as per gensim
    specs.

    :param files: list of text files
    :returns: document-term-matrix, dictionary and corpus
    """
    dtm = []
    for f in filenames:
        tokens = ut.tokenize(f)
        dtm.append(ut.filter_stopwords(tokens))
    from gensim import corpora
    dic = corpora.Dictionary(dtm)
    corp = [dic.doc2bow(doc) for doc in dtm]
    return dtm, dic, corp

def get_LSI_model(filenames, nt = 20):
    """Given a list of text files, returns an LSI model. Prints the topics by
    default.

    :param filenames: list of filenames
    :param nt: number of topics
    :returns: LSI model using gensim
    """
    from gensim import models
    dtm, dic, corp = build_dtm(filenames)
    lsi = models.lsimodel.LsiModel(corpus=corp, id2word=dic, num_topics=nt)
    for t in [lsi.show_topic(i) for i in range(nt)]:
        print [i[1] for i in t]
    return lsi

def get_LDA_model(filenames, nt = 20, chunks = 10, every = 1, np = 1):
    """Given a list of text files, returns an LSI model. Prints the topics by
    default.

    :param filenames: list of filenames
    :param nt: number of topics
    :param chunks: number of chunks
    :param every: update_every
    :param np: number of passes
    :returns: LSI model using gensim
    """
    from gensim import models
    dtm, dic, corp = build_dtm(filenames)
    lda = models.ldamodel.LdaModel(corpus=corp, id2word=dic, num_topics=nt, update_every=every, chunksize=10, passes=np)
    for t in [lda.show_topic(i) for i in range(nt)]:
        print [i[1] for i in t]
    return lda

if __name__ == '__main__':
    #lsi = get_LSI_model(['body' + str(i) for i in range(244)])
    lda = get_LDA_model(['body' + str(i) for i in range(244)], nt = 50)
