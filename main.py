import corpusbuilder as cb
import textutils as tu

def test1():
    """Test to check if entity extraction works correctly
    """
    docs = []
    corpus = cb.Corpus()
    for i in range(25):
        fr = open('body' + str(i))
        text = fr.read()
        doc = cb.Document(docid = 'body' + str(i), doctext = text)
        docs.append(doc)
        corpus.add_document(doc)
    ef = corpus.get_entities()
    for e in ef:
        print e + ',' + str(ef[e])
    print corpus    

def test2():
    """Test to check build_occurrences
    """
    fr = open('body24')
    text = fr.read()
    doc = cb.Document(docid = 'body3', doctext =text)
    doc.build_occurrences()
    for occurrence in doc.get_occurrences():
        print occurrence + ': [' + ','.join([str(context) for context in\
                                             doc.get_occurrences()[occurrence]])\
                                             + ']'

if __name__ == '__main__':
    test2()
