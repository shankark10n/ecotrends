"""
List of routines to obtain entities from a text file.
"""
import textutils as tu

def noun_phrase_xctor(text):
    """Given text, calls nounphrase.get_nounphrases

    :param text: text (one big string)
    :returns: list of entities
    """
    import nounphrase as np
    return np.get_nounphrases(text)

def nltk_collocations_xctor(text, n = 10):
    """Given text, calls NLTK's bigram collocations routine. By default, the
    method uses the likelihood ratio scoring fn to filter out and return
    only the top bigrams

    :param text: text (one big string)
    :param n: number of bigrams to return
    :returns: list of entities
    """
    from nltk.collocations import BigramCollocationFinder as BCF
    from nltk.metrics import BigramAssocMeasures as BAM
    import textutils as tu
    words = tu.tokenize_text(text)
    words = tu.filter_stopwords(words)
    bcf = BigramCollocationFinder.from_words(words)
    bigrams = [i[0] + ' ' + i[1] for i in bcf.nbest(BAM.likelihood_ratio, n)]
    return bigrams

def wiki_ngram_xctor(text, n = 2,\
                     path = '/tmp/wiki/enwiki-latest-all-titles-in-ns0-part',\
                     compress = False):
    """Given text, looks up Wikipedia article titles. If there's a match,
    that's an entity and returns the list of n-grams.

    :param text: text
    :param n: if 2, bigrams, if 3, trigrams
    :param path: where the Wiki titles files are; along with common prefix
    :returns: set of all n-grams that are also Wikipedia article titles.
    """
    import gzip
    import textutils as tu
    from collections import defaultdict as dd
    final = dd(int)
    for i in range(1,11):
        if compress:
            wpf = open(path + str(i) + '.gz', 'rb')
        else:
            wpf = open(path + str(i))
        titles = wpf.readlines()
        # Convert to dictionary for easy referencing
        titles_dict = dict([(' '.join(title.strip().lower().split('_')), True)\
                        for title in titles])
        ngrams = tu.get_ngrams(text, n)
        for ngram in ngrams:
            if ngram in titles_dict:
                final[ngram] += 1
        wpf.close()

    return final

if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print 'Usage: python %s <file to extract entities from>' % sys.argv[0]
        sys.exit(0)
    fr = open(sys.argv[1])
    text = fr.read()
    #ng1 = set([ng.lower() for ng in noun_phrase_xctor(text)])
    ng2 = wiki_ngram_xctor(text)
    #print 'NP \intersect wiki: %s' % ng1.intersection(ng2)
    #print 'NP \difference wiki: %s' % ng1.difference(ng2)
    #print 'Wiki \difference NP: %s' % ng2.difference(ng1)
    for ng in ng2:
        print ng
