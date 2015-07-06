#mdbTest.py
import sys, random, math, itertools
import monetdb.sql as mdb
import time
from numpy import *
import Gnuplot, Gnuplot.funcutils

def decToBinTrans(dec, numCols, numChunks):
	
	binLen = math.ceil(numCols + math.log(numChunks, 2))
	binCode = bin(dec)[2:]
	if(len(binCode) < binLen):
		lbc = len(binCode)
		for x in range(binLen - lbc):
			binCode = "0" + binCode

def binToRecTrans(bin, numCols):

	#input binary representation, return list of columns, chunk

	bin = str(bin)

	col = [i for i in range(numCols) if(bin[i] == '1')]
	chunk = int(bin[numCols:], 2)

	return col, chunk

def recToBinTrans(col, chunk, numCols, numChunks):
	
	chunkBinLen = math.ceil(math.log(numChunks, 2))

	#input list of columns relevant, chunk number

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
	sizeChunk = math.floor(numRows/numChunks)
	#sizeChunk = math.ceil(numRows/numChunks)

	cur.execute("SELECT * FROM " + table)
	colList = [x[0] for x in  cur.description]

	#level 1
	for x in range(numChunks):
		createTable(cur, conn, 'dc_' + table + "c" + str(x), 6, 1)
		for i in range(1, len(colList)):

			#cur.execute("CREATE FUNCTION GET_CHUNK(lim INT, off INT, tbl varchar(32), col varchar(32)) RETURNS TABLE (clm integer)"
			#	+" RETURN SELECT col FROM tbl LIMIT lim OFFSET off; END;")
			##^^This is the statement that SHOULD work but doesn't because monetdb doesn't recognize the variables like "col", "lim"
			
			cur.execute("CREATE FUNCTION GET_CHUNK() RETURNS TABLE (clm integer) "
				+"BEGIN RETURN SELECT " + colList[i] + " FROM " + table + " LIMIT " + str(sizeChunk) + " OFFSET " + str(x*sizeChunk) + "; END;")
			
			cur.execute("SELECT AVG(clm), STDDEV_SAMP(clm), VAR_SAMP(clm), MEDIAN(clm) FROM GET_CHUNK()")

			#cur.execute("SELECT AVG(banana) FROM GET_CHUNK()")
			avg, std, var, med = cur.fetchone()

			#cur.execute("SELECT TOP 1 COUNT( ) val, freq FROM " + table + " GROUP BY " + colList[i] + " ORDER BY COUNT( ) DESC")
			#mod = int(cur.fetchone()[0])
			mod = 0
			cur.execute("INSERT INTO dc_" + table + "c" + str(x) + " (col0, col1, col2, col3, col4, col5) VALUES (%s, %s, %s, %s, %s, %s)", 
				[int(recToBinTrans([i], x, numCols, numChunks), 2), avg, std,var,med,mod])
			cur.execute("DROP FUNCTION GET_CHUNK()")

	#level 2
	for i, j in itertools.combinations(range(1, numCols+1), 2):
		for c in range(numChunks):

			cur.execute("CREATE FUNCTION GET_CHUNK() RETURNS TABLE (cl1 integer, cl2 integer) "
			+"BEGIN RETURN SELECT " + colList[i] + "," + colList[j] + " FROM " + table + " LIMIT " + str(sizeChunk) + " OFFSET " + str(x*sizeChunk) + "; END;")
			cur.execute("SELECT CORR(cl1, cl2) FROM GET_CHUNK()")
			cur.execute("INSERT INTO dc_" + table + "c" + str(c) + " (col0, col1) VALUES (%s, %s)", 
				[int(recToBinTrans([i, j], c, numCols, numChunks), 2), cur.fetchone()[0]])
			cur.execute("DROP FUNCTION GET_CHUNK()")
	conn.commit()

	#3-n Levels
	for i in range(3, levels+1):
		for c in range(numChunks):
			for j in itertools.combinations(range(1, numCols + 1), i):
				vals = []
				for k in itertools.combinations(range(1, i), i-1):
					cur.execute("SELECT col1 FROM dc_" + table + "c" + str(c) + " WHERE col0=" 
						+ str(int(recToBinTrans(k, c, numCols, numChunks), 2)))
					vals.append(cur.fetchone()[0])

				correlation = sum(vals) + 42

				cur.execute("INSERT INTO dc_" + table + "c" + str(c) + " (col0, col1) VALUES (%s, %s)", 
					[str(int(recToBinTrans(j, c, numCols, numChunks), 2)), correlation])

			conn.commit()

def createDCLevels(cur, conn, table, levels, numChunks, numCols, numRows):

	createTable(cur, conn, 'dc_' + table + "1", 6, 1)
	maxRows = (2**numCols - 1)*numChunks
	sizeChunk = math.floor(numRows/numChunks)
	#sizeChunk = math.ceil(numRows/numChunks)

	cur.execute("SELECT * FROM " + table)
	colList = [x[0] for x in  cur.description]

	#level 1
	for i in range(1, len(colList)):
		for x in range(numChunks):

			#cur.execute("CREATE FUNCTION GET_CHUNK(lim INT, off INT, tbl varchar(32), col varchar(32)) RETURNS TABLE (clm integer)"
			#	+" RETURN SELECT col FROM tbl LIMIT lim OFFSET off; END;")
			##^^This is the statement that SHOULD work but doesn't because monetdb doesn't recognize the variables "col", "tbl", "off", "lim"
			
			cur.execute("CREATE FUNCTION GET_CHUNK() RETURNS TABLE (clm integer) "
				+"BEGIN RETURN SELECT " + colList[i] + " FROM " + table + " LIMIT " + str(sizeChunk) + " OFFSET " + str(x*sizeChunk) + "; END;")
			
			cur.execute("SELECT AVG(clm), STDDEV_SAMP(clm), VAR_SAMP(clm), MEDIAN(clm) FROM GET_CHUNK()")

			#cur.execute("SELECT AVG(banana) FROM GET_CHUNK()")
			avg, std, var, med = cur.fetchone()

			#cur.execute("SELECT TOP 1 COUNT( ) val, freq FROM " + table + " GROUP BY " + colList[i] + " ORDER BY COUNT( ) DESC")
			#mod = int(cur.fetchone()[0])
			mod = 0
			cur.execute("INSERT INTO dc_" + table + "1 (col0, col1, col2, col3, col4, col5) VALUES (%s, %s, %s, %s, %s, %s)", 
				[int(recToBinTrans([i], x, numCols, numChunks), 2), avg, std,var,med,mod])
			cur.execute("DROP FUNCTION GET_CHUNK()")

	#level 2
	createTable(cur, conn, 'dc_' + table + "2", 6, 1)
	for i, j in itertools.combinations(range(1, numCols+1), 2):
		for c in range(numChunks):
			cur.execute("CREATE FUNCTION GET_CHUNK() RETURNS TABLE (cl1 integer, cl2 integer) "
			+"BEGIN RETURN SELECT " + colList[i] + "," + colList[j] + " FROM " + table + " LIMIT " + str(sizeChunk) + " OFFSET " + str(x*sizeChunk) + "; END;")
			cur.execute("SELECT CORR(cl1, cl2) FROM GET_CHUNK()")
			cur.execute("INSERT INTO dc_" + table + "2 (col0, col1) VALUES (%s, %s)", 
				[int(recToBinTrans([i, j], c, numCols, numChunks), 2), cur.fetchone()[0]])
			cur.execute("DROP FUNCTION GET_CHUNK()")
	conn.commit()

	#3-n Levels
	for i in range(3, levels+1):
		createTable(cur, conn, 'dc_' + table + str(i), 6, 1)
		for c in range(numChunks):
			for j in itertools.combinations(range(1, numCols + 1), i):
				vals = []
				for k in itertools.combinations(range(1, i), i-1):
					cur.execute("SELECT col1 FROM dc_" + table + str(i-1) + " WHERE col0=" 
						+ str(int(recToBinTrans(k, c, numCols, numChunks), 2)))
					vals.append(cur.fetchone()[0])

				correlation = sum(vals) + 42

				cur.execute("INSERT INTO dc_" + table + str(i) + " (col0, col1) VALUES (%s, %s)", 
					[str(int(recToBinTrans(j, c, numCols, numChunks), 2)), correlation])

			conn.commit()

def createDCTable(cur, conn, table, levels, numChunks, numCols, numRows):

	timing = []

	startTime = time.time()

	if(numCols + math.log(numChunks, 2) >= 64):
		createTable(cur, conn, 'dc_' + table, 6, 1, 1)
	else:
		createTable(cur, conn, 'dc_' + table, 6, 1)

	maxRows = (2**numCols - 1)*numChunks
	sizeChunk = math.floor(numRows/numChunks)
	#sizeChunk = math.ceil(numRows/numChunks)

	cur.execute("SELECT * FROM " + table)
	colList = [x[0] for x in  cur.description]

	timing.append(time.time() - startTime)
	startTime = time.time()

	#level 1
	for i in range(1, len(colList)):
		for x in range(numChunks):

			#cur.execute("CREATE FUNCTION GET_CHUNK(lim INT, off INT, tbl varchar(32), col varchar(32)) RETURNS TABLE (clm integer)"
			#	+" RETURN SELECT col FROM tbl LIMIT lim OFFSET off; END;")
			##^^This is the statement that SHOULD work but doesn't because monetdb doesn't recognize the variables like "col", "lim"
			
			cur.execute("CREATE FUNCTION GET_CHUNK() RETURNS TABLE (clm integer) "
				+"BEGIN RETURN SELECT " + colList[i] + " FROM " + table + " LIMIT " + str(sizeChunk) + " OFFSET " + str(x*sizeChunk) + "; END;")
			
			#cur.execute("SELECT AVG(clm), STDDEV_SAMP(clm), VAR_SAMP(clm), MEDIAN(clm) FROM GET_CHUNK()")

			#removed median for consistency

			cur.execute("SELECT AVG(clm), STDDEV_SAMP(clm), VAR_SAMP(clm) FROM GET_CHUNK()")

			#avg, std, var, med = cur.fetchone()
			avg, std, var = cur.fetchone()

			med = 0

			#cur.execute("SELECT TOP 1 COUNT( ) val, freq FROM " + table + " GROUP BY " + colList[i] + " ORDER BY COUNT( ) DESC")
			#mod = int(cur.fetchone()[0])
			mod = 0

			cur.execute("INSERT INTO dc_" + table + " (col0, col1, col2, col3, col4, col5) VALUES (%s, %s, %s, %s, %s, %s)", 
				[recToBinTrans([i], x, numCols, numChunks), avg, std,var,med,mod])
			cur.execute("DROP FUNCTION GET_CHUNK()")

	timing.append(time.time() - startTime)
	startTime = time.time()

	print("reached 2")

	#level 2
	for i, j in itertools.combinations(range(1, numCols+1), 2):
		for c in range(numChunks):
			cur.execute("CREATE FUNCTION GET_CHUNK() RETURNS TABLE (cl1 bigint, cl2 bigint) "
			+ "BEGIN RETURN SELECT " + colList[i] + "," + colList[j] + " FROM " + table 
			+ " LIMIT " + str(sizeChunk) + " OFFSET " + str(x*sizeChunk) + "; END;")
			
			cur.execute("SELECT CORR(cl1, cl2) FROM GET_CHUNK()")

			cur.execute("INSERT INTO dc_" + table + " (col0, col1) VALUES (%s, %s)", 
				[recToBinTrans([i, j], c, numCols, numChunks), cur.fetchone()[0]])
			cur.execute("DROP FUNCTION GET_CHUNK()")

	conn.commit()

	timing.append(time.time() - startTime)
	startTime = time.time()

	print("reached 3")

	#3-n Levels
	for i in range(3, levels+1):
		for c in range(numChunks):
			for j in itertools.combinations(range(1, numCols + 1), i):
				vals = []
				for k in itertools.combinations(range(1, i), i-1):
					banana = str(recToBinTrans(k, c, numCols, numChunks))
					cur.execute("SELECT col1 FROM dc_" + table + " WHERE col0='" 
					#	+ str(int(recToBinTrans(k, c, numCols, numChunks), 2)))
						+ recToBinTrans(k, c, numCols, numChunks) + "'")
					vals.append(cur.fetchone()[0])

				correlation = sum(vals) + 42

				cur.execute("INSERT INTO dc_" + table + " (col0, col1) VALUES (%s, %s)", 
				#	[str(int(recToBinTrans(j, c, numCols, numChunks), 2)), correlation])
					[str(recToBinTrans(j, c, numCols, numChunks)), correlation])
			conn.commit()
 
	timing.append(time.time() - startTime)

	return timing

def createTable(cur, conn, name, numCol, p=0, l=0):

	if(p == 1):
		if(l == 1):
			cols = "(col0 blob PRIMARY KEY,"
		else:
			cols = "(col0 blob PRIMARY KEY,"
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

	cur.execute("SELECT * FROM " + table)
	colList = [x[0] for x in  cur.description]

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

		cur.execute(exe, [random.randint(1, 5) for x in range(len(colList))])


def getAllData(cur, conn, table):
	cur.execute("SELECT * FROM " + table)
	print(cur.fetchall())

def main():

	#DC INFO
	numChunks = 5
	numCols = 5
	numRows = 100
	levels = numCols

	conn = mdb.connect(username="monetdb", password="monetdb", database="test")
	cur = conn.cursor()

	if(sys.argv[1] == "get"):
		getAllData(cur, conn, sys.argv[2])
	elif(sys.argv[1] == "insert"):
		insertRandData(cur, conn, sys.argv[2], sys.argv[3])
	elif(sys.argv[1] == "graph"):
		graphData(cur, conn, sys.argv[2], sys.argv[3])
	elif(sys.argv[1] == "create"):
		createTable(cur, conn, sys.argv[2], sys.argv[3])
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
	levels = numCols

	conn = mdb.connect(username="monetdb", password="monetdb", database="test")
	cur = conn.cursor()


	createTable(cur, conn, "test", 6)
	insertRandData(cur, conn, "test", 100)
	timing = createDCTable(cur, conn, "test", levels, numChunks, numCols, numRows)

	print(timing)

#if __name__=="__main__": startTime = time.time(); main()
if __name__=="__main__": startTime = time.time(); test()