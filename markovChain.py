import numpy as np

class MarkovChain(object):
    """A markov chain object. Holds the data of the graph as a dictionary
    and has the probabilities associated with each edge as an array.

    {
        state: current state
        stateDict: {state: [[array of edges],[associated probilites]]}
    }
    """

    def __init__(self, stateDict, startState):
        """Constructor for the class. stateDict is the dictionary and
        start state is what state to start at"""
        self._stateDict =  stateDict
        self._state = startState

    def getState(self):
        """Prints the current state"""
        return self._state

    def nextState(self):
        """Randomly walks to the next state by the assigned probabilites in 
        the dictionary"""
        probabilites = list(self._stateDict[self._state])
        self._state = np.random.choice(a=probabilites[0],
                p=probabilites[1])

