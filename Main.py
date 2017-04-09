import nltk, re, pprint
from nltk import word_tokenize
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Markov Model")
sc = SparkContext(conf=conf)
from graphframes import *
import string
from MarkovChain import MarkovChain
import MarkovModel
"""The main driver for creating the markov chain and the sentences"""

#enter your books into this array
bookArray = []

#enter number of sentences here
numSentences = 5

#enter word you would like to start with
startWord = 'A'
markovModel = MarkovModel(bookArray)
edgeDF = markovModel.createEdgeDF()
verticiesDF = markovModel.getVerticiesDF()
graph = GraphFrame(verticiesDF, edgeDF)

if startWord not in verticiesDF.collect.id:
    print("The word you entered does not appear in any of the texts!")

######BENEATH THIS STILL NEEDS TO BE FIXED AND CHANGED TO DATAFRAMES
######ALSO MARKOV CHAIN OBJECT 
else:
    markovChain = MarkovChain(stateDict, startState)
    while numSentences > 0:
        currentState = markovChain.getState()
        if currentState in string.punctuation:
            numSentences-=1
        print(markovChain.getState())
        markovChain.nextState()



