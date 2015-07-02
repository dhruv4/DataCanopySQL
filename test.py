import sys, random, math, os
import configparser
import psycopg2 as pg
import monetdb.sql as mdb
import time
from numpy import *
import Gnuplot, Gnuplot.funcutils
import matplotlib.pyplot as plt
import pgTest, mdbTest

def graph(x, t, xtitle, name, db):

	if not os.path.exists("pg"):
		os.makedirs("pg")

	if not os.path.exists("mdb"):
		os.makedirs("mdb")

	f = open(db + '/gp_' + name + '_' + db + '_' + xtitle +  '.txt', 'w')
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
	'''
	#g.title(xtitle + " vs Time (sec)")
	#g('set style data lines')
	#g.ylabel('Time (Sec)')
	#g.xlabel(xtitle)

	#g.plot([[x[i], t[i]] for i in range(len(x))])

	#g.fit([[x[i], t[i]] for i in range(len(x))])

	#g.hardcopy(db + '/gp_' + name + '_' + db + '_' + xtitle + '.ps', enhanced=1, color=1)

	print("x", x, "t", t, name)

	plt.plot(x, t, 'bo')

	plt.title(xtitle + " vs Time (sec)")
	plt.ylabel('Time (Sec)')
	plt.xlabel(xtitle)
	#plt.plot([[x[i], t[i]] for i in range(len(x))])
	
	plt.savefig(db + '/gp_' + name + '_' + db + '_' + xtitle + '.png')

def Plot(filename,x_value,y_values,outfile):

	#open_file = pd.read_csv(filename)
	#x = np.array(open_file[x_value])

	# instantiate gnuplot
	g = gp.Gnuplot()

	# initialize an array for plot data
	plot_data = []

	# create gnuplot data
	#for y_value in y_values:
	#	y = np.array(open_file[y_value])
	#	y_label = y_value.replace("_"," ")
	#	plot_data.append(gp.Data(x,y,with_="lp",title=y_label))


	# set the labels
	#x_label = x_value.replace("_"," ")

	gp.Data(x,y,with_="lp",title=y_label)

	#formatting options
	g("set grid")
	g("set key left")
	g("set terminal pdf enhanced font 'times,12'")
	g("set output '"+outfile+"'")
	#g("set format y '%sx10^{%S}'")
	g("set xlabel '"+x_label+"'")
	g("set format y '%1.2e'")



	#plot
	#g.plot(plot_data)
	#g.plot([[x[i], t[i]] for i in range(len(x))])

def runExperiment():
		
	#to combine everything into one file, maybe use a dictionary with 'pg' or 'mdb' as keys, leading to an array

	Config = configparser.ConfigParser()
	Config.read("config.ini")
	####^^CHANGE THE CONFIG FILE TO CHANGE VARIABLES

	numTrials = Config.getint("Experiment Config", "NumberOfTrials")

	numChunks = Config.getint("Data Canopy Config", "NumChunks")
	numRows = Config.getint("Data Set Config", "NumRows")
	numStats = Config.getint("Data Canopy Config", "NumStats")
	numCols = Config.getint("Data Set Config", "NumCols")
	numLevels = Config.getint("Data Canopy Config", "NumLevels")

	times = []
	vals = []

	#find which sys.arg is "x" and that one's gonna be the variable????????

	r = int(math.ceil(math.log(numCols, 2)))
	####^^CHANGE THIS TO CHANGE VARIABLE

	for i in range(1, r+1):

		numCols = 2**i
		####^^CHANGE THIS TO CHANGE VARIABLE

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

		for x in timing:
			timing[x] /= numTrials
		times.append(timing)
		
		vals.append(numCols)
		print("trial", numCols, "ran")
		####^^CHANGE THIS TO CHANGE VARIABLE

		cur.execute("DROP TABLE exp")
		conn.commit()
		cur.close()
		conn.close()

	print("vals", vals)
	print("times", times)
	
	for j in timing:

		graph(vals, [k[j] for k in times], Config.get("Experiment Config", "XAxis"), Config.get("Experiment Config", "Title") + j, sys.argv[1])

def runAccessExperiment():

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

	#for x in range(numLevels):
	#	for i in range(numTrials):
	#		access
	return

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