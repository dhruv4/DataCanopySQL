import sys, random, math, os
import configparser
import psycopg2 as pg
import monetdb.sql as mdb
from time import clock
from numpy import *
import Gnuplot, Gnuplot.funcutils
import pgTest, mdbTest

def graph(x, t, xtitle, name, db):

	if not os.path.exists("pg"):
		os.makedirs("pg")

	if not os.path.exists("mdb"):
		os.makedirs("mdb")

	g = Gnuplot.Gnuplot()
	g.title(xtitle + " vs Time (sec)")
	g('set style data lines fill solid 1.0 border -1')
	g.ylabel('Time (Sec)')
	g.xlabel(xtitle)

	g.plot([[x[i], t[i]] for i in range(len(x))])

	#g.fit([[x[i], t[i]] for i in range(len(x))])

	g.hardcopy(db + '/gp_' + name + '_' + db + '.ps', enhanced=1, color=1)

def runExperiment():
		
	#to combine everything into one file, maybe use a dictionary with 'pg' or 'mdb' as keys, leading to an array

	Config = configparser.ConfigParser()
	Config.read("config.ini")

	numTrials = Config.getint("Experiment Config", "NumberOfTrials")

	numChunks = Config.getint("Data Canopy Config", "NumChunks")
	numRows = Config.getint("Data Set Config", "NumRows")
	numStats = Config.getint("Data Canopy Config", "NumStats")
	numCols = Config.getint("Data Set Config", "NumCols")
	numLevels = Config.getint("Data Canopy Config", "NumLevels")

	times = []
	vals = []

	#find which sys.arg is "x" and that one's gonna be the variable????????

	if(sys.argv[1] == "pg"):

		conn = pg.connect(dbname="postgres")
		cur = conn.cursor()
		pgTest.createTable(cur, conn, 'exp', numCols + 1)
		pgTest.insertRandData(cur, conn, 'exp', numRows)

	elif(sys.argv[1] == "mdb"):

		conn = mdb.connect(username="monetdb", password="monetdb", database="test")
		cur = conn.cursor()
		mdbTest.createTable(cur, conn, 'exp', numCols + 1)
		mdbTest.insertRandData(cur, conn, 'exp', numRows)
	
	conn.commit()

	for i in range(numTrials):

		startTime = clock()
		if(sys.argv[1] == "pg"):

			timing = pgTest.createDCTable(cur, conn, 'exp', numLevels, numChunks, numCols, numRows)

		elif(sys.argv[1] == "mdb"):

			timing = mdbTest.createDCTable(cur, conn, 'exp', numLevels, numChunks, numCols, numRows)

		timing['total'] = clock()-startTime
		times.append(timing)
		cur.execute("DROP TABLE dc_exp")
		conn.commit()
		vals.append(i)
		print("trial", i, "ran")

	cur.execute("DROP TABLE exp")
	conn.commit()
	cur.close()
	conn.close()

	print("vals", vals)
	print("times", times)
	
	for j in timing:

		graph(vals, [k[j] for k in times], Config.get("Experiment Config", "XAxis"), Config.get("Experiment Config", "Title") + j, sys.argv[1])

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