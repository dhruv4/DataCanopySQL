import sys, random, math, os
import configparser
import psycopg2 as pg
import monetdb.sql as mdb
import time
from numpy import *
#import Gnuplot, Gnuplot.funcutils
import matplotlib.pyplot as plt
import pgTest, mdbTest

def graph(x, t, xtitle, name, db, ylog=0):

	if not os.path.exists("pgresults"):
		os.makedirs("pgresults")

	if not os.path.exists("mdbresults"):
		os.makedirs("mdbresults")

	f = open(db + 'results/' + name + '_' + db + '_' + xtitle +  '.txt', 'w')
	for i in range(len(x)):
		f.write(str(x[i]) + ',' + str(t[i]) + '\n')
	f.close()
	'''
	g = Gnuplot.Gnuplot()


	outfile = db + '/gp_' + name + '_' + db + '_' + xtitle + '.ps'

	g("set grid")
	g("set key left")
	g("set terminal pdf enhanced font 'times,12'")
	g("set output '"+outfile+"'")
	#g("set format y '%sx10^{%S}'")
	g("set xlabel '"+xtitle+"'")
	g("set format y '%1.2e'")

	g.title(xtitle + " vs Time (sec)")
	g('set style data lines')
	g.ylabel('Time (Sec)')
	g.xlabel(xtitle)

	g.plot([[x[i], t[i]] for i in range(len(x))])

	g.fit([[x[i], t[i]] for i in range(len(x))])

	g.hardcopy(db + '/gp_' + name + '_' + db + '_' + xtitle + '.ps', enhanced=1, color=1)
	'''
	print("x", x, "t", t, name)

	if(name[4:]=="setup"):
		plt.plot(x, t, '-yo', label=name[4:])
	if(name[4:]=="level1"):
		plt.plot(x, t, '-r+', label=name[4:])
	if(name[4:]=="level2"):
		plt.plot(x, t, '-gx', label=name[4:])
	if(name[4:]=="leveln"):
		plt.plot(x, t, '-b*', label=name[4:])
	if(name[4:]=="total"):
		plt.plot(x, t, '-ms', label=name[4:])

	plt.legend(loc="upper left")
	if(xtitle == "Rows"):
		plt.xscale('log')

	plt.title(xtitle + " vs Time (sec)")
	plt.ylabel('Time (Sec)')
	plt.tight_layout()
	plt.xlabel(xtitle)
	
	if(ylog == 1):
		plt.yscale('log')
		plt.savefig(db + 'results/mpl_' + db + '_' + xtitle + 'log.pdf')
	else:
		plt.savefig(db + 'results/mpl_' + db + '_' + xtitle + '.pdf')

def runExperiment():
	
	Config = configparser.ConfigParser()
	################################################
	Config.read("test.ini")
	####^^CHANGE THE CONFIG FILE TO CHANGE VARIABLES
	################################################

	numTrials = Config.getint("Experiment Config", "NumberOfTrials")

	numChunks = Config.getint("Data Canopy Config", "NumChunks")
	numRows = Config.getint("Data Set Config", "NumRows")
	numStats = Config.getint("Data Canopy Config", "NumStats")
	numCols = Config.getint("Data Set Config", "NumCols")
	numLevels = Config.getint("Data Canopy Config", "NumLevels")
	xaxis = Config.get("Experiment Config", "XAxis")

	times = []
	vals = []

	if(xaxis == "Chunks"):
		r = int(math.ceil(math.log(numChunks, 10)))
		a = 1
	elif(xaxis == "Cols"):
		r = numCols/5
		a = 2
	elif(xaxis == "Rows"):
		r = int(math.ceil(math.log(numRows, 10)))
		a = 4

	for i in range(a, r+1):

		if(xaxis == "Cols"):
			numCols = 5*i
		elif(xaxis == "Rows"):
			numRows = 10**i
		elif(xaxis == "Chunks"):
			numChunks = 10**i
		
		numLevels = numCols

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

		timing = {}
		timing['setup'] = 0
		timing['level1'] = 0
		timing['level2'] = 0
		timing['leveln'] = 0
		timing['total'] = 0

		for j in range(numTrials):

			startTime = time.time()

			if(sys.argv[1] == "pg"):

				s, one, two, n = pgTest.createDCTable(cur, conn, 'exp', numLevels, numChunks, numCols, numRows)

				timing['setup'] += s
				timing['level1'] += one
				timing['level2'] += two
				timing['leveln'] += n

			elif(sys.argv[1] == "mdb"):

				s, one, two, n = mdbTest.createDCTable(cur, conn, 'exp', numLevels, numChunks, numCols, numRows)
				timing['setup'] += s
				timing['level1'] += one
				timing['level2'] += two
				timing['leveln'] += n
				
			timing['total'] += time.time()-startTime
			cur.execute("DROP TABLE dc_exp")
			conn.commit()

			print(j)

		for x in timing:
			timing[x] /= numTrials
		times.append(timing)
		
		if(xaxis == "Cols"):
			vals.append(numCols)
			print("trial", numCols, "ran")
		elif(xaxis == "Rows"):
			vals.append(numRows)
			print("trial", numRows, "ran")
		elif(xaxis == "Chunks"):
			vals.append(numChunks)
			print("trial", numChunks, "ran")

		cur.execute("DROP TABLE exp")
		conn.commit()
		cur.close()
		conn.close()

	print("vals", vals)
	print("times", times)
	
	for j in timing:

		graph(vals, [k[j] for k in times], xaxis, Config.get("Experiment Config", "Title") + j, sys.argv[1])

	plt.close()

	for j in timing:

		graph(vals, [k[j] for k in times], xaxis, Config.get("Experiment Config", "Title") + j, sys.argv[1], 1)
	
	plt.close()
		
def runAccessExperiment():

	numTrials = 100
	numChunks = 5
	numCols = 16
	numLevels = numCols
	numRows = 1000

	#test access
	if(sys.argv[1] == "pg"):

		conn = pg.connect(dbname="postgres")
		cur = conn.cursor()
		pgTest.createTable(cur, conn, 'exp', numCols + 1)
		pgTest.insertRandData(cur, conn, 'exp', numRows)
		pgTest.createDCTable(cur, conn, 'exp', numLevels, numChunks, numCols, numRows)

	elif(sys.argv[1] == "mdb"):

		conn = mdb.connect(username="monetdb", password="monetdb", database="test")
		cur = conn.cursor()
		mdbTest.createTable(cur, conn, 'exp', numCols + 1)
		mdbTest.insertRandData(cur, conn, 'exp', numRows)
		mdbTest.createDCTable(cur, conn, 'exp', numLevels, numChunks, numCols, numRows)
	
	conn.commit()	

	timing = []

	for x in range(numLevels):
		timeSum = 0
		for i in range(numTrials):
			startTime = time.time()

			randList = []

			while(len(randList) < x+1):
				num = random.randint(1, numLevels+1)
				if(num not in randList):
					randList.append(num)

			if(sys.argv[1] == "mdb"):
				cur.execute("SELECT * FROM dc_exp WHERE col0 = " + str(mdbTest.recToBinTrans(randList, random.randint(0, numChunks))))
			else:
				cur.execute("SELECT * FROM dc_exp WHERE col0 = cast('" + pgTest.recToBinTrans(randList, random.randint(0, numChunks)) + "', as varbit)")

			timeSum += time.time() - startTime

		timing.append(timeSum/numTrials)

	graph(range(numLevels), timing, "levels", "AccessTest", sys.argv[1])

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
	print("Run time: ", time.time() - startTime, " seconds")

#if __name__=="__main__": startTime = time.time(); main()
if __name__=="__main__": startTime = time.time(); runExperiment()
