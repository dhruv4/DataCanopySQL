import sys, random, math, os
import configparser
import psycopg2 as pg
import monetdb.sql as mdb
import time
from numpy import *
import Gnuplot, Gnuplot.funcutils
import matplotlib.pyplot as plt
import pgCache, mdbCache

def graph(x, t, xtitle, name, db, c=0, ylog=0):

	if not os.path.exists("pgresults"):
		os.makedirs("pgresults")

	if not os.path.exists("mdbresults"):
		os.makedirs("mdbresults")

	if(c == 1):
		f = open(db + 'results/' + name + '_' + db + '_cache_' + xtitle +  '.txt', 'w')
	else:
		f = open(db + 'results/' + name + '_' + db + '_time_' + xtitle +  '.txt', 'w')

	for i in range(len(x)):
		f.write(str(x[i]) + ',' + str(t[i]) + '\n')
	f.close()
	
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

	if(c == 1):	
		plt.title(xtitle + " vs Cache Misses")
		plt.ylabel('Cache Misses')
	else:
		plt.title(xtitle + " vs Time (sec)")
		plt.ylabel('Time (Sec)')
	plt.tight_layout()
	plt.xlabel(xtitle)
	
	if(ylog == 1):
		if(c == 1):
			plt.yscale('log')
			plt.savefig(db + 'results/mpl_' + db + '_cache_' + xtitle + 'log.pdf')
		else:
			plt.yscale('log')
			plt.savefig(db + 'results/mpl_' + db + '_time_' + xtitle + 'log.pdf')
	else:
		if(c == 1):
			plt.savefig(db + 'results/mpl_' + db + '_cache_' + xtitle + '.pdf')
		else:
			plt.savefig(db + 'results/mpl_' + db + '_time_' + xtitle + '.pdf')

def runExperiment():
	
	Config = configparser.ConfigParser()
	################################################
	Config.read("config.ini")
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
	caches = []
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
		
		#numLevels = numCols

		startTime = time.time()

		if(sys.argv[1] == "pg"):

			conn = pg.connect(dbname="postgres")
			cur = conn.cursor()
			pgCache.createTable(cur, conn, 'exp', numCols + 1)
			#pgCache.insertRandData(cur, conn, 'exp', numRows)
			cur.execute("COPY exp FROM '/home/gupta/DataCanopySQL/test" + str(numRows) + ".csv' DELIMITER ',' CSV")
			#cur.execute("COPY exp FROM '/home/gupta/DataCanopySQL/test" + str(numRows) + ".npy' WITH BINARY")

		elif(sys.argv[1] == "mdb"):

			conn = mdb.connect(username="monetdb", password="monetdb", database="test")
			cur = conn.cursor()
			mdbCache.createTable(cur, conn, 'exp', numCols + 1)
			#mdbCache.insertRandData(cur, conn, 'exp', numRows)
			cur.execute("COPY INTO exp FROM '/home/gupta/DataCanopySQL/test" + str(numRows) + ".csv' USING DELIMITERS ','")
			#cur.execute("COPY INTO exp FROM 'test" + str(numRows) + ".npy'")
		
		conn.commit()

		print("Table loaded", time.time() - startTime)

		timing = {}
		timing['setup'] = 0
		timing['level1'] = 0
		timing['level2'] = 0
		timing['leveln'] = 0
		timing['total'] = 0
		caching = {}
		caching['setup'] = 0
		caching['level1'] = 0
		caching['level2'] = 0
		caching['leveln'] = 0
		caching['total'] = 0

		for j in range(numTrials):

			if(sys.argv[1] == "pg"):

				os.system("rm -rf filename.txt")

				totalStart = time.time()

				startTime = time.time()
				os.system("perf stat -e 'cache-misses' -x- python3 pgCache.py setup exp " + str(numLevels) + " " + str(numChunks) + " " + str(numCols) + " " + str(numRows) + " >> filename.txt 2>&1")
				timing['setup'] += time.time() - startTime
				print("reached 1")
				startTime = time.time()
				os.system("perf stat -e 'cache-misses' -x- python3 pgCache.py level1 exp " + str(numLevels) + " " + str(numChunks) + " " + str(numCols) + " " + str(numRows) + " >> filename.txt 2>&1")
				timing['level1'] += time.time() - startTime
				print("reached 2")
				startTime = time.time()
				os.system("perf stat -e 'cache-misses' -x- python3 pgCache.py level2 exp " + str(numLevels) + " " + str(numChunks) + " " + str(numCols) + " " + str(numRows) + " >> filename.txt 2>&1")
				timing['level2'] += time.time() - startTime
				print("reached n")
				print("numLevels", numLevels)
				startTime = time.time()
				if(numLevels > 2):
					os.system("perf stat -e 'cache-misses' -x- python3 pgCache.py leveln exp " + str(numLevels) + " " + str(numChunks) + " " + str(numCols) + " " + str(numRows) + " >> filename.txt 2>&1")
				timing['leveln'] += time.time() - startTime

				timing['total'] += time.time() - totalStart

				lines = [line.rstrip('\n') for line in open('filename.txt')]

				for line in lines:
					if(line[-12:] == "cache-misses"):
						caching['setup'] += int(line.split('-')[0])
						break

				lines.remove(line)

				for line in lines:
					if(line[-12:] == "cache-misses"):
						caching['level1'] += int(line.split('-')[0])
						break

				lines.remove(line)

				for line in lines:
					if(line[-12:] == "cache-misses"):
						caching['level2'] += int(line.split('-')[0])
						break

				lines.remove(line)

				for line in lines:
					if(line[-12:] == "cache-misses"):
						caching['leveln'] += int(line.split('-')[0])
						break

				lines.remove(line)

			elif(sys.argv[1] == "mdb"):

				os.system("rm -rf filename.txt")

				totalStart = time.time()

				startTime = time.time()

				os.system("perf stat -e 'cache-misses' -x- python3 mdbCache.py setup exp " + str(numLevels) + " " + str(numChunks) + " " + str(numCols) + " " + str(numRows) + " >> filename.txt 2>&1")
				timing['setup'] += time.time() - startTime
				print("reached 1")
				startTime = time.time()
				os.system("perf stat -e 'cache-misses' -x- python3 mdbCache.py level1 exp " + str(numLevels) + " " + str(numChunks) + " " + str(numCols) + " " + str(numRows) + " >> filename.txt 2>&1")
				timing['level1'] += time.time() - startTime
				print("reached 2")
				startTime = time.time()
				os.system("perf stat -e 'cache-misses' -x- python3 mdbCache.py level2 exp " + str(numLevels) + " " + str(numChunks) + " " + str(numCols) + " " + str(numRows) + " >> filename.txt 2>&1")
				timing['level2'] += time.time() - startTime
				print("numLevels", numLevels)
				startTime = time.time()
				if(numLevels > 2):
					os.system("perf stat -e 'cache-misses' -x- python3 mdbCache.py leveln exp " + str(numLevels) + " " + str(numChunks) + " " + str(numCols) + " " + str(numRows) + " >> filename.txt 2>&1")
				timing['leveln'] += time.time() - startTime

				timing['total'] += time.time() - totalStart

				lines = [line.rstrip('\n') for line in open('filename.txt')]

				for line in lines:
					if(line[-12:] == "cache-misses"):
						caching['setup'] += int(line.split('-')[0])
						break

				lines.remove(line)

				for line in lines:
					if(line[-12:] == "cache-misses"):
						caching['level1'] += int(line.split('-')[0])
						break

				lines.remove(line)

				for line in lines:
					if(line[-12:] == "cache-misses"):
						caching['level2'] += int(line.split('-')[0])
						break

				lines.remove(line)

				for line in lines:
					if(line[-12:] == "cache-misses"):
						caching['leveln'] += int(line.split('-')[0])
						break

				lines.remove(line)
	
			caching['total'] += caching['setup'] + caching['level1'] + caching['level2'] + caching['leveln']
			#^SUM OF THE CACHE MISSES

			cur.execute("SELECT COUNT(*) FROM dc_exp")
			print("Size of Data Canopy: ", cur.fetchone()[0])
			print("Predicted Size of DC: ", numChunks*(2**numCols - 1))
			cur.execute("DROP TABLE dc_exp")
			conn.commit()

			print(j)

		for x in caching:
			caching[x] /= numTrials
		caches.append(caching)

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
	print("caches", caches)
	
	for j in caching:

		graph(vals, [k[j] for k in caches], Config.get("Experiment Config", "XAxis"), Config.get("Experiment Config", "Title") + j, sys.argv[1], 1)

	plt.close()

	for j in caching:

		graph(vals, [k[j] for k in caches], Config.get("Experiment Config", "XAxis"), Config.get("Experiment Config", "Title") + j, sys.argv[1], 1, 1)
	
	plt.close()

	for j in timing:

		graph(vals, [k[j] for k in times], Config.get("Experiment Config", "XAxis"), Config.get("Experiment Config", "Title") + j, sys.argv[1], 0)

	plt.close()

	for j in timing:

		graph(vals, [k[j] for k in times], Config.get("Experiment Config", "XAxis"), Config.get("Experiment Config", "Title") + j, sys.argv[1], 0, 1)
	
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
