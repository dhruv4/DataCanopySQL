#pgCache.py
import sys, random, math, itertools
import psycopg2 as pg
import time
from numpy import *
import Gnuplot, Gnuplot.funcutils

def binToRecTrans(bin, numCols):

	#input binary representation, return list of columns, chunk

	bin = str(bin)

	col = [i for i in range(numCols) if(bin[i] == '1')]
	chunk = int(bin[numCols:], 2)

	return col, chunk

def recToBinTrans(col, chunk, numCols, numChunks):

	#input list of columns relevant, chunk number
	binLen = math.ceil(numCols + math.log(numChunks, 2))
	chunkBinLen = math.ceil(math.log(numChunks, 2))

	colBin = ""

	for x in range(numCols + 1):
		if(x in col):
			colBin += '1'
		else:
			colBin += '0'

	chunkBin = bin(chunk)[2:]
	if(len(chunkBin) < chunkBinLen):
		lcb = len(chunkBin)
		for x in range(chunkBinLen - lcb):
			chunkBin = "0" + chunkBin

	return colBin + chunkBin

def createDCChunks(cur, conn, table, levels, numChunks, numCols, numRows):
	
	maxRows = (2**numCols - 1)*numChunks
	#sizeChunk = math.ceil(numRows/numChunks)
	sizeChunk = math.floor(numRows/numChunks)

	cur.execute("SELECT column_name from information_schema.columns where table_name='" + table + "'");
	colList = [x[0] for x in cur.fetchall()]

	#level 1 Postgres
	
	for x in range(numChunks):
		createTable(cur, conn, 'dc_' + table + "c" + str(x), 6, 1)
		for j in range(1, numCols+1):

			cur.execute("SELECT AVG(ss), STDDEV(ss), VAR_SAMP(ss) FROM (SELECT " 
				+ colList[j] + " AS ss FROM " 
				+ table + " LIMIT " + str(sizeChunk) 
				+ " OFFSET " + str(x*sizeChunk) + ") as foo")
			avg, stddev, var = cur.fetchone()

			med = 0 #median????

			#cur.execute("SELECT TOP 1 COUNT( ) val, freq FROM " + table + " GROUP BY " + colList[j] + " ORDER BY COUNT( ) DESC")
			#mod = int(cur.fetchone()[0])
			mod = 0
			cur.execute("INSERT INTO dc_" + table + "c" + str(x) + " (col0, col1, col2, col3, col4, col5) VALUES (%s, %s, %s, %s, %s, %s)",
				[recToBinTrans([j], x, numCols, numChunks), avg, stddev,var,med,mod])

	#level 2 DC

	for i, j in itertools.combinations(range(1, numCols+1), 2):
		for c in range(numChunks):
			cur.execute("SELECT CORR(x, y) FROM (SELECT cast(" + colList[j] + " as double precision) AS x, cast(" 
				+ colList[i] + " as double precision) AS y FROM " 
				+ table + " LIMIT " + str(sizeChunk) 
				+ " OFFSET " + str(c*sizeChunk) + ") as foo")
			cur.execute("INSERT INTO dc_" + table + "c" + str(c) + " (col0, col1) VALUES (%s, %s)", 
				[recToBinTrans([i, j], c, numCols, numChunks),float(cur.fetchone()[0])])

	#3-n Levels
	for i in range(3, levels+1):
		for c in range(numChunks):
			for j in itertools.combinations(range(1, numCols + 1), i):
				vals = []
				for k in itertools.combinations(range(1, i), i-1):
					cur.execute("SELECT col1 FROM dc_" + table + "c" + str(c) + " WHERE col0 = cast('" 
						+ recToBinTrans(k, c, numCols, numChunks) + "' as varbit)")
					vals.append(cur.fetchone()[0])				

				correlation = sum(vals) + 42

				cur.execute("INSERT INTO dc_" + table + "c" + str(c) + " (col0, col1) VALUES (%s, %s)", 
					[recToBinTrans(j, c, numCols, numChunks), correlation])

def createDCLevels(cur, conn, table, levels, numChunks, numCols, numRows):
	
	maxRows = (2**numCols - 1)*numChunks
	#sizeChunk = math.ceil(numRows/numChunks)
	sizeChunk = math.floor(numRows/numChunks)
	
	createTable(cur, conn, 'dc_' + table + '1', 6, 1)

	cur.execute("SELECT column_name from information_schema.columns where table_name='" + table + "'");
	colList = [x[0] for x in cur.fetchall()]

	#level 1 Postgres
	for j in range(1, numCols+1):
		for x in range(numChunks):

			cur.execute("SELECT AVG(ss), STDDEV(ss), VAR_SAMP(ss) FROM (SELECT " 
				+ colList[j] + " AS ss FROM " 
				+ table + " LIMIT " + str(sizeChunk) 
				+ " OFFSET " + str(x*sizeChunk) + ") as foo")
			avg, stddev, var = cur.fetchone()

			med = 0 #median????

			#cur.execute("SELECT TOP 1 COUNT( ) val, freq FROM " + table + " GROUP BY " + colList[j] + " ORDER BY COUNT( ) DESC")
			#mod = int(cur.fetchone()[0])
			mod = 0
			cur.execute("INSERT INTO dc_" + table + "1 (col0, col1, col2, col3, col4, col5) VALUES (%s, %s, %s, %s, %s, %s)",
				[recToBinTrans([j], x, numCols, numChunks), avg, stddev,var,med,mod])

	#level 2 DC

	createTable(cur, conn, 'dc_' + table + '2', 6, 1)
	for i, j in itertools.combinations(range(1, numCols+1), 2):
		for c in range(numChunks):
			cur.execute("SELECT CORR(x, y) FROM (SELECT cast(" + colList[j] + " as double precision) AS x, cast(" 
				+ colList[i] + " as double precision) AS y FROM " 
				+ table + " LIMIT " + str(sizeChunk) 
				+ " OFFSET " + str(c*sizeChunk) + ") as foo")
			cur.execute("INSERT INTO dc_" + table + "2 (col0, col1) VALUES (%s, %s)", 
				[recToBinTrans([i, j], c, numCols, numChunks),float(cur.fetchone()[0])])

	#3-n Levels
	for i in range(3, levels+1):
		createTable(cur, conn, 'dc_' + table + str(i), 6, 1)
		for c in range(numChunks):
			for j in itertools.combinations(range(1, numCols + 1), i):
				vals = []
				for k in itertools.combinations(range(1, i), i-1):
					cur.execute("SELECT col1 FROM dc_" + table + str(i-1) + " WHERE col0 = cast('" 
						+ recToBinTrans(k, c, numCols, numChunks) + "' as varbit)")
					vals.append(cur.fetchone()[0])				

				correlation = sum(vals) + 42

				cur.execute("INSERT INTO dc_" + table + str(i) + " (col0, col1) VALUES (%s, %s)", 
					[recToBinTrans(j, c, numCols, numChunks), correlation])

def createDCTableSetup(table, levels, numChunks, numCols, numRows):
	
	conn = pg.connect(dbname="postgres")
	cur = conn.cursor()

	createTable(cur, conn, 'dc_' + table, 6, 1)

	conn.commit()

def createDCTableLevel1(table, levels, numChunks, numCols, numRows):

	conn = pg.connect(dbname="postgres")
	cur = conn.cursor()

	cur.execute("SELECT column_name from information_schema.columns where table_name='" + table + "'");
	colList = [x[0] for x in cur.fetchall()]

	maxRows = (2**numCols - 1)*numChunks
	#sizeChunk = math.ceil(numRows/numChunks)
	sizeChunk = math.floor(numRows/numChunks)

	#level 1 Postgres
	for j in range(1, numCols+1):
		for x in range(numChunks):

			cur.execute("SELECT AVG(ss), STDDEV(ss), VAR_SAMP(ss) FROM (SELECT " 
				+ colList[j] + " AS ss FROM " 
				+ table + " LIMIT " + str(sizeChunk) 
				+ " OFFSET " + str(x*sizeChunk) + ") as foo")
			avg, stddev, var = cur.fetchone()

			med = 0 #median????

			#cur.execute("SELECT TOP 1 COUNT( ) val, freq FROM " + table + " GROUP BY " + colList[j] + " ORDER BY COUNT( ) DESC")
			#mod = int(cur.fetchone()[0])
			mod = 0
			cur.execute("INSERT INTO dc_" + table + " (col0, col1, col2, col3, col4, col5) VALUES (%s, %s, %s, %s, %s, %s)",
				[recToBinTrans([j], x, numCols, numChunks), avg, stddev,var,med,mod])

	conn.commit()
	
def createDCTableLevel2(table, levels, numChunks, numCols, numRows):
	
	conn = pg.connect(dbname="postgres")
	cur = conn.cursor()

	cur.execute("SELECT column_name from information_schema.columns where table_name='" + table + "'");
	colList = [x[0] for x in cur.fetchall()]

	maxRows = (2**numCols - 1)*numChunks
	#sizeChunk = math.ceil(numRows/numChunks)
	sizeChunk = math.floor(numRows/numChunks)

	#level 2 DC
	print("reached 2")

	for i, j in itertools.combinations(range(1, numCols+1), 2):
		for c in range(numChunks):
			cur.execute("SELECT CORR(x, y) FROM (SELECT cast(" + colList[j] + " as double precision) AS x, cast(" 
				+ colList[i] + " as double precision) AS y FROM " 
				+ table + " LIMIT " + str(sizeChunk) 
				+ " OFFSET " + str(c*sizeChunk) + ") as foo")

			cur.execute("INSERT INTO dc_" + table + " (col0, col1) VALUES (%s, %s)", 
				[recToBinTrans([i, j], c, numCols, numChunks),float(cur.fetchone()[0])])

	conn.commit()

def createDCTableLeveln(table, levels, numChunks, numCols, numRows, two = 0):

	conn = pg.connect(dbname="postgres")
	cur = conn.cursor()

	#3-n Levels
	for i in range(3, levels+1):
		print("reached", i)
		comb = list(itertools.combinations(range(1, numCols + 1), i))
		for j in comb:
			for cval in range(numChunks):
				vals = []
				if(two == 1):
					comb2 = list(itertools.combinations(j, 2))
				else:
					comb2 = list(itertools.combinations(j, i-1))
				for k in comb2:
					cur.execute("SELECT col1 FROM dc_" + table + " WHERE col0 = cast('" 
						+ recToBinTrans(k, cval, numCols, numChunks) + "' as varbit)")
					vals.append(cur.fetchone()[0])				

				correlation = sum(vals) + 42

				cur.execute("INSERT INTO dc_" + table + " (col0, col1) VALUES (%s, %s)", 
					[recToBinTrans(j, cval, numCols, numChunks), correlation])
		conn.commit()

def createTable(cur, conn, name, numCol, b=0):

	if(b == 1):
		cols = "(col0 varbit PRIMARY KEY,"
		for x in range(1, numCol):
			cols += "col" + str(x) + " double precision,"
	else:
		cols = "("
		for x in range(numCol):
			cols += "col" + str(x) + " int,"
	

	cols = cols[:-1]

	cols += ")"

	cur.execute("CREATE TABLE " + name + " " + cols)

def graphData(cur, conn, table, col):
	
	cur.execute("SELECT " + col + " FROM " + table)
	g = Gnuplot.Gnuplot()
	g.title('A simple example')
	#g('set style data histograms')
	g('set style fill solid 1.0 border -1')

	#g.plot([[0,1.1], [1,5.8], [2,3.3], [3,4.2]])
	data = cur.fetchall()
	g.plot(data)
	g.hardcopy('gp_' + table + '.ps', enhanced=1, color=1)

def alterTable(cur, conn):
	return
def insertRandData(cur, conn, table, length):

	cur.execute("SELECT column_name from information_schema.columns where table_name='" + table + "'");
	colList = [x[0] for x in cur.fetchall()]

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

		cur.execute(exe, [random.randint(0, 5) for x in range(len(colList))])


def getAllData(cur, conn, table):
	cur.execute("SELECT * FROM " + table)
	print(cur.fetchall())

def main():
	#DC INFO
	numChunks = 5
	numCols = 5
	numRows = 100
	levels = numCols

	conn = pg.connect(dbname="postgres")
	cur = conn.cursor()

	if(sys.argv[1] == "get"):
		getAllData(cur, conn, sys.argv[2])
	elif(sys.argv[1] == "insert"):
		insertRandData(cur, conn, sys.argv[2], sys.argv[3])
	elif(sys.argv[1] == "graph"):
		graphData(cur, conn, sys.argv[2], sys.argv[3])
	elif(sys.argv[1] == "create"):
		createTable(cur, conn, sys.argv[2], int(sys.argv[3]))
	elif(sys.argv[1] == "createdc"):
		createDCTable(cur, conn, sys.argv[2], levels, numChunks, numCols, numRows)

	conn.commit()
	cur.close()
	conn.close()
	print("Run time: ", time.time() - startTime, " seconds")

def test():
	numChunks = 5
	numCols = 5
	numRows = 100

	conn = pg.connect(dbname="postgres")
	cur = conn.cursor()
	
	timing = createDCTable(cur, conn, "test", numCols, numChunks, numCols, numRows)

	conn.commit()
	print(timing)

def exp():
	
	if(sys.argv[1] == "setup"):
		createDCTableSetup(sys.argv[2], int(sys.argv[3]),int( sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]))
	elif(sys.argv[1] == "level1"):
		createDCTableLevel1(sys.argv[2], int(sys.argv[3]),int( sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]))
	elif(sys.argv[1] == "level2"):
		createDCTableLevel2(sys.argv[2], int(sys.argv[3]),int( sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]))
	elif(sys.argv[1] == "leveln"):
		createDCTableLeveln(sys.argv[2], int(sys.argv[3]),int( sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]))

#if __name__=="__main__": startTime = time.time(); main()
#if __name__=="__main__": startTime = time.time(); test()
if __name__=="__main__": startTime = time.time(); exp()
