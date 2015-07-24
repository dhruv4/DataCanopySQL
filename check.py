#check.py
import sys, random, math, itertools, time

levels = 15
numChunks = 10
numCols = 15
length = 10000000
table = "test"
colList = ["c" for x in range(numCols)]

startTime = time.time()
'''
for i in range(3, levels+1):
	comb = list(itertools.combinations(range(1, numCols + 1), i))
	for j in comb:
		for cval in range(numChunks):
			vals = []
			comb2 = list(itertools.combinations(j, i-1))
			for k in comb2:
				vals.append(46)
			correlation = sum(vals) + 42
'''
for x in range(int(length)):
	exe = "INSERT INTO " + table + " ("
	
	for x in colList:
		exe += x + ","

	exe = exe[:-1]
	exe += ") values ("

	for x in range(len(colList)):
		exe += "%s, "

	exe = exe[:-2]
	exe += ")"
	
	a = [random.randint(0, 5) for x in range(len(colList))]	

	#cur.execute(exe, [random.randint(0, 5) for x in range(len(colList))])

print(time.time() - startTime)
