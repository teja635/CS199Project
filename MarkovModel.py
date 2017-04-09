import nltk, re, pprint
from nltk import word_tokenize
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Markov Model")
sc = SparkContext(conf=conf)

class CreateDict(object): 
    def __init__(self, bookArray):
        self._bookArray = bookArray

    def createDict():
        for i in _bookArray:
            updateText(i)
        createDict()
        normalizeDict()
        return self._bigramCount.toDF()

    def updateText(newBook):
        with open(newBook,"r") as book:
            self._text += " " + book.read().replace("\n"," ").replace("\r","")

    def createDict(self):
        lines = sc.parallelize(_text)
        lines = lines.map(lambda x: x.lower())
        words = lines.map(lambda x: x.split())
        #tokens = lines.flatMap(lambda x: word_tokenize(x)) 
        bigrams = words.flatMap(lambda x: list((x[i],x[i + 1]) for i in range(len(x) - 1)))
        self._bigramCount = bigrams.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)

        
"""
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
"""

    def normalizeDict():
        # normalization of the data
        total = self._bigramCount.map(lambda x: x[1]).reduce(lambda x,y: x+y)
        self._bigramCount = self._bigramCount.map(lambda x: (x[0],x[1]/total))
        """
        for i in self._bigramCount:
            s = sum(self._word_map[i].values())
            if(s != 0):
                for k in self._word_map[i]:
                    self._word_map[i][k] /= s
"""

