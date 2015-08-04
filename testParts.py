import sys, random, math, os
import configparser
import psycopg2 as pg
import monetdb.sql as mdb
import time
from numpy import *
import matplotlib.pyplot as plt
import pgNew, mdbNew

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
		numChunks = int(sys.argv[3])
	elif(xaxis == "Cols"):
		numCols = int(sys.argv[3])
	elif(xaxis == "Rows"):
		numRows = int(sys.argv[3])
			
	startTime = time.time()

	if(sys.argv[1] == "pg"):

		conn = pg.connect(dbname="postgres")
		cur = conn.cursor()
		if(sys.argv[2] == "setup"):
			pgNew.createTable(cur, conn, 'exp', numCols)
			conn.commit()
			pgNew.insertRandData(cur, conn, 'exp', numRows)
			'''
			if(xaxis == "Cols"):
				cur.execute("COPY exp FROM '/home/gupta/DataCanopySQL/test" + str(numCols) + ".csv' DELIMITER ',' CSV")
			elif(xaxis == "Rows"):
				cur.execute("COPY exp FROM '/home/gupta/DataCanopySQL/test" + str(numRows) + ".csv' DELIMITER ',' CSV")
			elif(xaxis == "Chunks"):
				cur.execute("COPY exp FROM '/home/gupta/DataCanopySQL/test" + str(numRows) + ".csv' DELIMITER ',' CSV")
			'''

	elif(sys.argv[1] == "mdb" and sys.argv[2] == "setup"):

		conn = mdb.connect(username="monetdb", password="monetdb", database="test")
		cur = conn.cursor()
		if(sys.argv[2] == "setup"):
			mdbNew.createTable(cur, conn, 'exp', numCols)
			#mdbNew.insertRandData(cur, conn, 'exp', numRows)
			if(xaxis == "Cols"):
				cur.execute("COPY INTO exp FROM '/home/gupta/DataCanopySQL/test" + str(numCols) + ".csv' USING DELIMITERS ','")
			elif(xaxis == "Rows"):
				cur.execute("COPY INTO exp FROM '/home/gupta/DataCanopySQL/test" + str(numRows) + ".csv' USING DELIMITERS ','")
			elif(xaxis == "Chunks"):
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

			if(sys.argv[2] == "setup"):
				os.system("rm -rf filenamepg.txt")
				startTime = time.time()
				os.system("perf stat -e 'cache-misses,task-clock' -x- python3 pgNew.py setup exp " + str(numLevels) + " " + str(numChunks) + " " + str(numCols) + " " + str(numRows) + " >> filenamepg.txt 2>&1")
				timing['setup'] = time.time() - startTime
				f = open('pgresults/' + Config.get("Experiment Config", "Title") + 'setup_pg_time_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
				f.write(sys.argv[3] + "," + str(timing['setup']) + "\n")
				f.close()
				print("reached 1")

			if(sys.argv[2] == "level1"):
				startTime = time.time()
				os.system("perf stat -e 'cache-misses,task-clock' -x- python3 pgNew.py level1 exp " + str(numLevels) + " " + str(numChunks) + " " + str(numCols) + " " + str(numRows) + " >> filenamepg.txt 2>&1")
				timing['level1'] = time.time() - startTime
				f = open('pgresults/' + Config.get("Experiment Config", "Title") + 'level1_pg_time_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
				f.write(sys.argv[3] + "," + str(timing['level1']) + "\n")
				f.close()
				print("reached 2")

			if(sys.argv[2] == "level2"):
				startTime = time.time()
				os.system("perf stat -e 'cache-misses,task-clock' -x- python3 pgNew.py level2 exp " + str(numLevels) + " " + str(numChunks) + " " + str(numCols) + " " + str(numRows) + " >> filenamepg.txt 2>&1")
				timing['level2'] = time.time() - startTime
				f = open('pgresults/' + Config.get("Experiment Config", "Title") + 'level2_pg_time_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
				f.write(sys.argv[3] + "," + str(timing['level2']) + "\n")
				f.close()
				print("reached n")	

			if(sys.argv[2] == "leveln"):
				f = open('pgresults/' + Config.get("Experiment Config", "Title") + 'leveln_pg_time_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
				print("numLevels", numLevels)
				startTime = time.time()
				if(numLevels > 2):
					os.system("perf stat -e 'cache-misses,task-clock' -x- python3 pgNew.py leveln exp " + str(numLevels) + " " + str(numChunks) + " " + str(numCols) + " " + str(numRows) + " >> filenamepg.txt 2>&1")
				timing['leveln'] = time.time() - startTime

				f.write(sys.argv[3] + "," + str(timing['leveln']) + "\n")
				f.close()

				lines = [line.rstrip('\n') for line in open('filenamepg.txt')]

				for line in lines:
					if(line[-12:] == "cache-misses"):
						caching['setup'] = int(line.split('-')[0])
						f = open('pgresults/' + Config.get("Experiment Config", "Title") + 'setup_pg_cache_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
						f.write(sys.argv[3] + "," + str(caching['setup']) + "\n")
						f.close()
						l1 = line
						continue
					if(caching['setup'] > 1):
						timing['setup'] = float(line.split('-')[0])/1000
						break

				print(caching, timing, l1, line, lines)

				lines.remove(l1)
				lines.remove(line)

				for line in lines:
					if(line[-12:] == "cache-misses"):
						caching['level1'] = int(line.split('-')[0])
						f = open('pgresults/' + Config.get("Experiment Config", "Title") + 'level1_pg_cache_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
						f.write(sys.argv[3] + "," + str(caching['level1']) + "\n")
						f.close()
						l1 = line
						continue
					if(caching['level1'] > 1):
						timing['level1'] = float(line.split('-')[0])/1000
						break

				print(caching, timing, l1, line, lines)

				lines.remove(l1)
				lines.remove(line)

				for line in lines:
					if(line[-12:] == "cache-misses"):
						caching['level2'] = int(line.split('-')[0])
						f = open('pgresults/' + Config.get("Experiment Config", "Title") + 'level2_pg_cache_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
						f.write(sys.argv[3] + "," + str(caching['level2']) + "\n")
						f.close()
						l1 = line
						continue
					if(caching['level2'] > 1):
						timing['level2'] = float(line.split('-')[0])/1000
						break

				print(caching, timing, l1, line, lines)

				lines.remove(l1)
				lines.remove(line)

				for line in lines:
					if(line[-12:] == "cache-misses"):
						caching['leveln'] = int(line.split('-')[0])
						f = open('pgresults/' + Config.get("Experiment Config", "Title") + 'leveln_pg_cache_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
						f.write(sys.argv[3] + "," + str(caching['leveln']) + "\n")
						f.close()
						l1 = line
						continue
					if(caching['leveln'] > 1):
						timing['leveln'] = float(line.split('-')[0])/1000
						break

				print(caching, timing, l1, line, lines)

				lines.remove(l1)
				lines.remove(line)

				caching['total'] = caching['setup'] + caching['level1'] + caching['level2'] + caching['leveln']
				timing['total'] = timing['setup'] + timing['level1'] + timing['level2'] + timing['leveln']
				#^SUM OF THE CACHE MISSES

				f = open('pgresults/' + Config.get("Experiment Config", "Title") + 'total_pg_time_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
				f.write(sys.argv[3] + "," + str(timing['total']) + "\n")
				f.close()

				cur.execute("SELECT COUNT(*) FROM dc_exp")
				print("Size of Data Canopy: ", cur.fetchone()[0])
				print("Predicted Size of DC: ", numChunks*(2**numCols - 1))
				cur.execute("DROP TABLE dc_exp")
				conn.commit()
				
				print("time: ", timing['total'], "cache misses: ", caching['total'])

				cur.execute("DROP TABLE exp")

		elif(sys.argv[1] == "mdb"):

			os.system("rm -rf filenamemdb.txt")

			if(sys.argv[2] == "setup"):
				startTime = time.time()
				os.system("perf stat -e 'cache-misses,task-clock' -x- python3 mdbNew.py setup exp " + str(numLevels) + " " + str(numChunks) + " " + str(numCols) + " " + str(numRows) + " >> filenamemdb.txt 2>&1")
				timing['setup'] = time.time() - startTime
				f = open('mdbresults/' + Config.get("Experiment Config", "Title") + 'setup_mdb_time_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
				f.write(sys.argv[3] + "," + str(timing['setup']) + "\n")
				f.close()
				print("reached 1")

			if(sys.argv[2] == "level1"):
				startTime = time.time()
				os.system("perf stat -e 'cache-misses,task-clock' -x- python3 mdbNew.py level1 exp " + str(numLevels) + " " + str(numChunks) + " " + str(numCols) + " " + str(numRows) + " >> filenamemdb.txt 2>&1")
				timing['level1'] = time.time() - startTime
				f = open('mdbresults/' + Config.get("Experiment Config", "Title") + 'level1_mdb_time_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
				f.write(sys.argv[3] + "," + str(timing['level1']) + "\n")
				f.close()
				print("reached 2")

			if(sys.argv[2] == "level2"):
				startTime = time.time()
				os.system("perf stat -e 'cache-misses,task-clock' -x- python3 mdbNew.py level2 exp " + str(numLevels) + " " + str(numChunks) + " " + str(numCols) + " " + str(numRows) + " >> filenamemdb.txt 2>&1")
				timing['level2'] = time.time() - startTime
				f = open('mdbresults/' + Config.get("Experiment Config", "Title") + 'level2_mdb_time_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
				f.write(sys.argv[3] + "," + str(timing['level2']) + "\n")
				f.close()
				print("reached n")	

			if(sys.argv[2] == "leveln"):
				f = open('mdbresults/' + Config.get("Experiment Config", "Title") + 'leveln_mdb_time_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
				print("numLevels", numLevels)
				startTime = time.time()
				if(numLevels > 2):
					os.system("perf stat -e 'cache-misses,task-clock' -x- python3 mdbNew.py leveln exp " + str(numLevels) + " " + str(numChunks) + " " + str(numCols) + " " + str(numRows) + " >> filenamemdb.txt 2>&1")
				timing['leveln'] = time.time() - startTime

				f.write(sys.argv[3] + "," + str(timing['leveln']) + "\n")
				f.close()

				lines = [line.rstrip('\n') for line in open('filenamemdb.txt')]

				for line in lines:
					if(line[-12:] == "cache-misses"):
						caching['setup'] = int(line.split('-')[0])
						f = open('mdbresults/' + Config.get("Experiment Config", "Title") + 'setup_mdb_cache_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'w')
						f.write(sys.argv[3] + "," + str(caching['setup']) + "\n")
						f.close()
						l1 = line
						continue
					if(caching['setup'] > 1):
						timing['setup'] = float(line.split('-')[0])/1000
						break


				lines.remove(l1)
				lines.remove(line)

				for line in lines:
					if(line[-12:] == "cache-misses"):
						caching['level1'] = int(line.split('-')[0])
						f = open('mdbresults/' + Config.get("Experiment Config", "Title") + 'level1_mdb_cache_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
						f.write(sys.argv[3] + "," + str(caching['level1']) + "\n")
						f.close()
						l1 = line
						continue
					if(caching['level1'] > 1):
						timing['level1'] = float(line.split('-')[0])/1000
						break

				lines.remove(l1)
				lines.remove(line)

				for line in lines:
					if(line[-12:] == "cache-misses"):
						caching['level2'] = int(line.split('-')[0])
						f = open('mdbresults/' + Config.get("Experiment Config", "Title") + 'level2_mdb_cache_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
						f.write(sys.argv[3] + "," + str(caching['level2']) + "\n")
						f.close()
						l1 = line
						continue
					if(caching['level2'] > 1):
						timing['level2'] = float(line.split('-')[0])/1000
						break

				lines.remove(l1)
				lines.remove(line)

				for line in lines:
					if(line[-12:] == "cache-misses"):
						caching['leveln'] = int(line.split('-')[0])
						f = open('mdbresults/' + Config.get("Experiment Config", "Title") + 'leveln_mdb_cache_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
						f.write(sys.argv[3] + "," + str(caching['leveln']) + "\n")
						f.close()
						l1 = line
						continue
					if(caching['leveln'] > 1):
						timing['leveln'] = float(line.split('-')[0])/1000
						break

				lines.remove(l1)
				lines.remove(line)

				caching['total'] = caching['setup'] + caching['level1'] + caching['level2'] + caching['leveln']
				timing['total'] = timing['setup'] + timing['level1'] + timing['level2'] + timing['leveln']
				f = open('mdbresults/' + Config.get("Experiment Config", "Title") + 'total_mdb_time_' + Config.get("Experiment Config", "XAxis") +  '.txt', 'a')
				f.write(sys.argv[3] + "," + str(timing['total']) + "\n")
				f.close()
				#^SUM OF THE CACHE MISSES

				cur.execute("SELECT COUNT(*) FROM dc_exp")
				print("Size of Data Canopy: ", cur.fetchone()[0])
				print("Predicted Size of DC: ", numChunks*(2**numCols - 1))
				cur.execute("DROP TABLE dc_exp")
				conn.commit()
				
				print("time: ", timing['total'], "cache misses: ", caching['total'])

				cur.execute("DROP TABLE exp")

	conn.commit()
	cur.close()
	conn.close()

if __name__=="__main__": startTime = time.time(); runExperiment()