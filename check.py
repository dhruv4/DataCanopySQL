#check.py
import sys, random, math, itertools, time

levels = 20
numChunks = 10
numCols = 20

startTime = time.time()
for i in range(3, levels+1):
	comb = list(itertools.combinations(range(1, numCols + 1), i))
	for j in comb:
		for cval in range(numChunks):
			vals = []
			comb2 = list(itertools.combinations(j, i-1))
			for k in comb2:
				vals.append(46)
			correlation = sum(vals) + 42

print(time.time() - startTime)