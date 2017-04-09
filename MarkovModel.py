import nltk, re, pprint
from nltk import word_tokenize
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Markov Model")
sc = SparkContext(conf=conf)

class CreateDict(object): 
    def __init__(self, bookArray):
        self.words = None

    def createEdgeDF():
        """Create and returns the edge dataframe"""

        self.updateText()
        self.createDict()
        self.normalizeDict()

        #Puts the rdd into the format the dataframe needs
        self._bigramCount = self._bigramCount.map(lambda x: (x[0][0], x[0][1], x[1]))
        return self._bigramCount.toDF(["src","dst","prob"])

    def getVerticiesDF(self):
        """Returns the vertices of the graph (the distinct words in the text)"""

        if self.words != None:
            return self.words.distinct().map(lambda x: (x,1)).toDF(["id","value"])
        else:  #if words has not been created (just in case) create it
            lines = sc.parallelize(_text)
            lines = lines.map(lambda x: x.lower())
            self.words = lines.map(lambda x: x.split())
            return self.words.distinct().map(lambda x: (x,1)).toDF(["id","value"])


    def updateText(self, newBook):
        """Updates the text of all the books in the proper format."""

        self._text = ""

        #For each book add them to the array of text
        for newBook in self._bookArray: 
            with open(newBook,"r") as book:
                self._text += " " + book.read().replace("\n"," ").replace("\r","")
        self._text = self._text.split(".") #Splits text by sentence
        for i in range(len(self._text)):
            self._text[i] += " ." #The period is still important, so add it back


    def createRDD(self):
        """Creates the rdd of the edges and their probablity"""

        lines = sc.parallelize(_text)
        lines = lines.map(lambda x: x.lower())
        self.words = lines.map(lambda x: x.split())
        #tokens = lines.flatMap(lambda x: word_tokenize(x)) 
        bigrams = self.words.flatMap(lambda x: list((x[i],x[i + 1]) for i in range(len(x) - 1)))
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

    def normalizeDict(self):
        """Normalizes the edge probabilites"""

        total = self._bigramCount.map(lambda x: x[1]).reduce(lambda x,y: x+y)
        self._bigramCount = self._bigramCount.map(lambda x: (x[0],x[1]/total))
        """
        for i in self._bigramCount:
            s = sum(self._word_map[i].values())
            if(s != 0):
                for k in self._word_map[i]:
                    self._word_map[i][k] /= s
        """

