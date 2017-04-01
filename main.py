import string
from .MarkovChain import MarkovChain
from .MarkovModel import update_map
"""The main driver for creating the markov chain and the sentences"""

#enter your books into this array
bookArray = []

#enter number of sentences here
numSentences = 5

#enter word you would like to start with
startWord = 'A'

stateDict = update_map(bookAarray)

if startWord not in stateDict:
    print("The word you entered does not appear in any of the texts!")
else:
    markovChain = MarkovChain(stateDict, startState)
    while numSentences > 0:
        currentState = markovChain.getState()
        if currentState in string.punctuation:
            numSentences-=1
        print(markovChain.getState())
        markovChain.nextState()



