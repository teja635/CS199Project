import nltk, re, pprint
from nltk import word_tokenize
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Markov Model")
sc = SparkContext(conf=conf)

class CreateDict(object): 
    def __init__(self, bookArray):
        self._bookArray = bookArray
        self._word_map = {}

    def createDict():
        for i in _bookArray:
            createDictForBook(i)
        normalizeDict()
        return self._word_map

    def createDictForBook(book):
        lines = sc.textFile(book)
        lines = lines.map(lambda x: x.lower())
        tokens = lines.flatMap(lambda x: word_tokenize(x)) 

        tok = tokens.collect()
        for i in range(len(tok)-1):
            w1 = tok[i]
                w2 = tok[i+1]
                if w1 in self._word_map:
                    if w2 in self._word_map[w1]:
                        self._word_map[w1][w2] += 1
                    else:
                        self._word_map[w1][w2] = 1
                else:
                    self._word_map[w1] = {w2: 1}


    def normalizeDict():
        # normalization of the data
        for i in self._word_map:
            s = sum(self._word_map[i].values())
            if(s != 0):
                for k in self._word_map[i]:
                    self._word_map[i][k] /= s



