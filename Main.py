import nltk, re, pprint, os
from nltk import word_tokenize
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from graphframes import *
conf = SparkConf().setAppName("Markov Model")
sc = SparkContext(conf=conf)
#sc = SparkContext(conf = conf, pyFiles=[os.path.join(os.path.abspath(os.path.dirname(__file__)), 'MarkovModel.py'), os.path.join(os.path.abspath(os.path.dirname(__file__)), 'MarkovChain.py')])
spark = SparkSession(sc)
import string
from MarkovChain import MarkovChain
from MarkovModel import DFMaker
"""The main driver for creating the markov chain and the sentences"""

#enter your books into this array
bookArray = ["AChristmasCarol.txt","DavidCopperField.txt","OliverTwist.txt",
"GreatEspectations.txt","TaleOfTwoCities.txt"]
for i in range(len(bookArray)):
    bookArray[i] = "/home/team2/Project/" + bookArray[i]

#enter number of sentences here
numSentences = 12

#enter word you would like to start with
startWord = 'a'

markovModel = DFMaker(bookArray)
edgeDF = markovModel.createEdgeDF(sc)
verticiesDF = markovModel.getVerticiesDF(sc)
graphFrame = GraphFrame(verticiesDF, edgeDF)

sentences = ""
if verticiesDF[verticiesDF["id"] == startWord].collect() == []:
    print("The word you entered does not appear in any of the texts!")
else:
    markovChain = MarkovChain(graphFrame, startWord)
    while numSentences > 0:
        currentState = markovChain.getState()
        if currentState in string.punctuation:
            numSentences-=1
        sentences+= markovChain.getState() + " "
        markovChain.nextState()

print(sentences)
