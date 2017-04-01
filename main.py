import string
"""The main driver for creating the markov chain and the sentences"""

#enter your books into this array
bookArray = []
#enter number of sentences here
numSentences = 5
#enter word you would like to start with
startWord = 'A'
#call Teja's code here


markovChain = MarkovChain(stateDict, startState)
prev = ''
while numSentences > 0:
    currentState = markovChain.getState()
    if currentState in string.punctuation:
        numSentences-=1
    print(markovChain.getState())
    markovChain.nextState()



