import os
import random
import re
from stemming.porter2 import stem
import sys

def satisfactory(ngram, freq_list, taboo, percentile, cumulative):
    """
    Returns true if an ngram from a list is 'satisfactory':
    1. not already present in the file name (e.g., 'rainfall'
    in 'agriculture_rainfall')
    2. is not a number
    3. is one of the top (picks randomly after using a threshold)    
    """
    if sum(ngrams[ngrams.index(ngram):]) < percentile * threshold:
        return False
    if len(re.findall('^[0-9]', ngram)) > 0:
        return False
    return True

def ngram_in_collection(ngram, coll):
    """
    Check if ngram's components are in collection
    """
    s1 = set([stem(word) for word in ngram.split(' ')])
    s2 = set([stem(word) for word in coll])
    return (len(s1.intersection(s2)) > 0)

def is_stopword(ngram):
    """
    Check if ngram has words in stopword collection or even begin with
    any word in stopword set.
    """
    stopwords = ['adobe', 'end', 'obj', 'cs', 'macintosh', 'indesign',\
                 'tweet', 'reply', 'comment', 'contact', 'cookies',
                 'market', 'shipping', 'replies', 'amazon', 'facebook',
                 'compare', 'web', 'user', 'huffpost']
    words = ngram.split(' ')
    for w in words:
        if any([w.startswith(x) for x in stopwords]):
            return True
    return False

def not_number(ngram):
    """
    Check if ngram contains words that begin with number
    """
    return all([len(re.findall('^[0-9]', x)) == 0 for x in ngram.split(' ')])
    
def min_length(ngram, threshold = 3):
    """
    Check if each word in ngram has min length at least threshold.
    """
    return all([len(x) >= threshold for x in ngram.split(' ')])

def top_ngram_picker(fname, percentile = 0.75):
    """
    Takes a file containing ngrams and picks an ngram that is 'satisfactory'
    """
    filegrams = fname.split('_')
    fr = open(fname)
    freq_pos = 1
    if fname.endswith('bg'):
        freq_pos = 2
    lines = fr.readlines()
    # remove stopwords
    lines = [l for l in lines if not(is_stopword(l))]
    ngrams = [' '.join(line.split(' ')[:freq_pos]) for line in lines]
    freq = [int(line.split(' ')[freq_pos]) for line in lines]
    sum_freq = sum(freq)
    
    # collect indices of top lines (those who have lot of freq)
    top_lines = [freq.index(num) for num in freq if sum(freq[freq.index(num):])\
                 >= percentile * sum_freq]
    # pick only those that are not garbage numbers
    top_lines = [i for i in top_lines if not_number(ngrams[i])]
    # pick only those that aren't already words in the filename
    top_lines = [i for i in top_lines if not(ngram_in_collection(ngrams[i],\
                                                                 filegrams))]
    # pick only those words that aren't in a stopword collection
    top_lines = [i for i in top_lines if not(is_stopword(ngrams[i]))]

    # pick only those words that have some min length threshold
    top_lines = [i for i in top_lines if min_length(ngrams[i])]
    
    if len(top_lines) == 0:
        return ''
    return ngrams[random.choice(top_lines)]

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Usage: %s <filename>. Example: %s agricultural_bg' % (sys.argv[0], sys.argv[0])
        sys.exit(0)
    print top_ngram_picker(sys.argv[1])
