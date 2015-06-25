import sys, random, math
import psycopg2 as pg
import monetdb.sql as mdb
from time import clock
from numpy import *
import Gnuplot, Gnuplot.funcutils
import pgTest, mdbTest

def graph(x, t, xtitle, name):

	g = Gnuplot.Gnuplot()
	g.title(xt + " vs Time")
	g('set style fill solid 1.0 border -1')

	g.plot([[x[i], t[i]] for i in len(x)])

	g.hardcopy('gp_' + name + '.ps', enhanced=1, color=1)

def runExperiment():
	
	if(sys.argv[1] == "pg"):

		conn = pg.connect(dbname="postgres")
		cur = conn.cursor()

	elif(sys.argv[1] == "mdb"):

		conn = mdb.connect(username="monetdb", password="monetdb", database="test")
		cur = conn.cursor()

	sizeOfTable = 1000
	
	times = []
	vals = []
	createTable(cur, conn, 'exp', sizeOfTable)
	conn.commit()
	for x in range(length):

		if(val == 0):
			numChunks = x
		elif(val == 1):
			numLevels = x
		elif(val == 2):
			numStats = x
		elif(val == 3):
			sizeOfTable = x

		startTime = clock()
		createDCTable(cur, conn, 'exp')
		vals.append(x)
		times.append(clock()-startTime)

	graph(vals, times, 'title', 'experiment')
	cur.execute("DROP TABLE exp")
	conn.commit()

def main():

	if(sys.argv[1] == "pg"):

		conn = pg.connect(dbname="postgres")
		cur = conn.cursor()

	elif(sys.argv[1] == "mdb"):

		conn = mdb.connect(username="monetdb", password="monetdb", database="test")
		cur = conn.cursor()

	if(sys.argv[2] == "get"):
		getAllData(cur, conn, sys.argv[3])
	elif(sys.argv[2] == "insert"):
		insertRandData(cur, conn, sys.argv[3], sys.argv[4], sys.argv[1])
	elif(sys.argv[2] == "graph"):
		graphData(cur, conn, sys.argv[3], sys.argv[4])
	elif(sys.argv[2] == "create"):
		createTable(cur, conn, sys.argv[3], sys.argv[4])
	elif(sys.argv[2] == "createdc"):
		createDCTable(cur, conn, sys.argv[3])

	conn.commit()
	cur.close()
	conn.close()
	print("Run time: ", clock() - startTime, " seconds")

#if __name__=="__main__": startTime = clock(); main()
if __name__=="__main__": startTime = clock(); runExperiment()
#########
# plan: 1. get multiple levels done
# 			-----------level 2 - have correlations 
# 			-----------level n - have correlations across n columns CHECK
#			-TURN TABLE INTO DOUBLE PRECISION????????????
#		2. ---------experimentation functions - graph x against y (probably time) AND change what varies with for loops
#		3. Change how table structures are stored - chunks vs levels
#		4. Experiment on accessing data with queries and stuff
#########
