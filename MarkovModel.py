"""
import nltk, re, pprint
from nltk import word_tokenize
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = SparkConf().setAppName("Markov Model")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
"""
import re
class DFMaker(object): 
    def __init__(self, bookArray):
        self._bookArray = bookArray
        self.words = None

    def createEdgeDF(self, sc):
        """Create and returns the edge dataframe"""

        self.updateText()
        self.createRDD(sc)
        self.normalizeDict()

        #Puts the rdd into the format the dataframe needs
        self._bigramCount = self._bigramCount.map(lambda x: (x[0][0], x[0][1], x[1]))
        return self._bigramCount.toDF(["src","dst","prob"])

    def getVerticiesDF(self, sc):
        """Returns the vertices of the graph (the distinct words in the text)"""

        if self.words != None:
            return self.words.flatMap(lambda x: x).distinct().map(lambda x: (x,1)).toDF(["id","value"])
        else:  #if words has not been created (just in case) create it
            lines = sc.parallelize(self._text)
            lines = lines.map(lambda x: x.lower())
            self.words = lines.map(lambda x: x.split())
            return self.words.distinct().flatMap(lambda x: x).map(lambda x: (x,1)).toDF(["id","value"])


    def updateText(self):
        """Updates the text of all the books in the proper format."""

        self._text = ""

        #For each book add them to the array of text
        for newBook in self._bookArray: 
            with open(newBook,"r") as book:
                self._text += " " + book.read().replace("\n"," ").replace("\r","")
        ####self._text = self._text.split(".") #Splits text by sentence
        self._text = re.split("\.!?",self._text) 
        for i in range(len(self._text)):
            self._text[i] += " ." #The period is still important, so add it back


    def createRDD(self,sc):
        """Creates the rdd of the edges and their probablity"""

        lines = sc.parallelize(self._text)
        lines = lines.map(lambda x: x.lower())
        self.words = lines.map(lambda x: x.split())
        #tokens = lines.flatMap(lambda x: word_tokenize(x)) 
        bigrams = self.words.flatMap(lambda x: list((x[i],x[(i + 1)% len(x)]) for i in range(len(x))))
        self._bigramCount = bigrams.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)

        

    def normalizeDict(self):
        """Normalizes the edge probabilites"""

        total = self._bigramCount.map(lambda x: (x[0][0], x[1])).reduceByKey(lambda x,y: x+y).collect()
        totalDict = {}
        for i in total:
            totalDict[i[0]] = i[1]
        self._bigramCount = self._bigramCount.map(lambda x: (x[0],x[1]/totalDict[x[0][0]]))

