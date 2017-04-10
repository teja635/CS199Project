import numpy as np

class MarkovChain(object):
    """A markov chain object. Holds the data of the graph as a dictionary
    and has the probabilities associated with each edge as an array.

    {
        state: current state
        stateDict: {state: [[array of edges],[associated probilites]]}
    }
    """

    def __init__(self, graphFrame, startState):
        """Constructor for the class. stateDict is the dictionary and
        start state is what state to start at"""
        self._graphFrame =  graphFrame
        print(startState)
        self._state = startState
        print(self._state)

    def getState(self):
        """Returns the current state"""
        return self._state

    def nextState(self):
        """Randomly walks to the next state by the assigned probabilites in 
        the dictionary"""
        probabilites = [i.prob for i in self._graphFrame.edges[self._graphFrame.edges['src'] == self._state].select("prob").collect()]
        dests = [i.dst for i in self._graphFrame.edges[self._graphFrame.edges['src'] == self._state].select("dst").collect()]
        if dests == []:
            return "."
        self._state = np.random.choice(a=dests,
                p=probabilites)

    def possibleStates(self, word, num=5):
        """returning the most probable future words for a given word"""
        p = sorted([(i.dst, i.prob) for i in self._graphFrame.edges[self._graphFrame.edges['src'] == word].collect()], key=lambda x:x[1], reverse=True)
        return p[0:min(num, len(p))]

    def generateTree(self, seed, depth=3):
        if depth == 0:
            return
        arr = []
        arr.append(seed)
        arr.append(possibleStates(3, seed))
        for i in range(len(arr[1])):
            arr[1][i] = (arr[1][i], generateTree(arr[1][i], depth-1))
        return arr
