import nltk, re, pprint
from nltk import word_tokenize
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Markov Model")
sc = SparkContext(conf=conf)

lines = sc.textFile("books/54445-0.txt")
lines = lines.map(lambda x: x.lower())
tokens = lines.flatMap(lambda x: word_tokenize(x)) 

word_map = {}

tok = tokens.collect()
for i in range(len(tok)-1):
	w1 = tok[i]
	w2 = tok[i+1]
	if w1 in word_map:
		if w2 in word_map[w1]:
			word_map[w1][w2] += 1
		else:
			word_map[w1][w2] = 1
	else:
		word_map[w1] = {w2: 1}
	

for i in word_map:
	print(i + " ->" +  str(word_map[i]))

# normalization of the data
for i in matrix:
	s = sum(i)
	if(s != 0):
		for k in range(len(i)):
			i[k] /= s


