#mdbNew.py
#pgNew.py
import sys, random, math, itertools
import monetdb.sql as mdb
import time
from numpy import *

def checkLevel1(x):
	while (((x % 2) == 0) and x > 1): #While x is even and > 1
		x >>= 1
	return (x == 1)

def checkLevel2(x):
	'''
	while (((x & 1) == 0) and x > 1): #/* While x is even and > 1 */
		x >>= 1
	return (x == 2)
	'''
	return bin(x).count('1') == 2

def createTable(cur, conn, name, numCol, b=0):

	if(b == 1):
		cols = "(col0 bigint PRIMARY KEY,"
		for x in range(1, numCol):
			cols += "col" + str(x) + " double precision,"
	else:
		cols = "("
		for x in range(numCol):
			cols += "col" + str(x) + " int,"
	

	cols = cols[:-1]

	cols += ")"

	cur.execute("CREATE TABLE " + name + " " + cols)

def createTable(cur, conn, name, numCol, p=0):

	if(p == 1):
		cols = "(col0 bigint PRIMARY KEY,"
		for x in range(1, numCol):
			cols += "col" + str(x) + " double precision,"
	else:
		cols = "("
		for x in range(numCol):
			cols += "col" + str(x) + " int,"

	cols = cols[:-1]

	cols += ")"

	cur.execute("CREATE TABLE " + name + " " + cols)

def idChunkCombine(idn, chunk, numChunks):
	return ((idn << math.ceil(math.log(numChunks, 2))) | chunk)

def createDCTableSetup(table, levels, numChunks, numCols, numRows):
	
	conn = mdb.connect(username="monetdb", password="monetdb", database="test")
	cur = conn.cursor()

	createTable(cur, conn, 'dc_' + table, 6, 1)

	conn.commit()

def createDCTableLevel1(table, levels, numChunks, numCols, numRows):

	conn = mdb.connect(username="monetdb", password="monetdb", database="test")
	cur = conn.cursor()

	cur.execute("SELECT * FROM " + table)
	colList = [x[0] for x in  cur.description]

	maxRows = (2**numCols - 1)*numChunks
	sizeChunk = math.ceil(numRows/numChunks)

	ID = 1
	for c in range(numChunks):
		for i in range(numCols):

			#cur.execute("CREATE FUNCTION GET_CHUNK(lim INT, off INT, tbl varchar(32), col varchar(32)) RETURNS TABLE (clm integer)"
			#	+" RETURN SELECT col FROM tbl LIMIT lim OFFSET off; END;")
			##^^This is the statement that SHOULD work but doesn't because monetdb doesn't recognize the variables like "col", "lim"
			
			cur.execute("CREATE FUNCTION GET_CHUNK() RETURNS TABLE (clm integer) "
				+"BEGIN RETURN SELECT " + colList[i] + " FROM " + table + " LIMIT " + str(sizeChunk) + " OFFSET " + str(c*sizeChunk) + "; END;")
			
			#cur.execute("SELECT AVG(clm), STDDEV_SAMP(clm), VAR_SAMP(clm), MEDIAN(clm) FROM GET_CHUNK()")

			#removed median for consistency

			cur.execute("SELECT AVG(clm), STDDEV_SAMP(clm), VAR_SAMP(clm) FROM GET_CHUNK()")

			#avg, std, var, med = cur.fetchone()
			avg, std, var = cur.fetchone()

			med = 0

			#cur.execute("SELECT TOP 1 COUNT( ) val, freq FROM " + table + " GROUP BY " + colList[i] + " ORDER BY COUNT( ) DESC")
			#mod = int(cur.fetchone()[0])
			mod = 0

			ID = 1<<i

			ID = idChunkCombine(ID, c, numChunks)

			cur.execute("INSERT INTO dc_" + table + " (col0, col1, col2, col3, col4, col5) VALUES (%s, %s, %s, %s, %s, %s)",
				[ID, avg, std,var,med,mod])

			cur.execute("DROP FUNCTION GET_CHUNK()")

	conn.commit()

def createDCTableLevel2(table, levels, numChunks, numCols, numRows):
	
	conn = mdb.connect(username="monetdb", password="monetdb", database="test")
	cur = conn.cursor()

	cur.execute("SELECT * FROM " + table)
	colList = [x[0] for x in  cur.description]

	maxRows = (2**numCols - 1)*numChunks
	sizeChunk = math.ceil(numRows/numChunks)

	for c in range(numChunks):
		for i in range(numCols - 1):
			for j in range(i+1, numCols):

				cur.execute("CREATE FUNCTION GET_CHUNK() RETURNS TABLE (cl1 bigint, cl2 bigint) "
					+ "BEGIN RETURN SELECT " + colList[i] + "," + colList[j] + " FROM " + table 
					+ " LIMIT " + str(sizeChunk) + " OFFSET " + str(c*sizeChunk) + "; END;")
				
				cur.execute("SELECT CORR(cl1, cl2) FROM GET_CHUNK()")

				cur.execute("INSERT INTO dc_" + table + " (col0, col1) VALUES (%s, %s)", 
					[idChunkCombine(2**i + 2**j, c, numChunks),float(cur.fetchone()[0])])

				cur.execute("DROP FUNCTION GET_CHUNK()")

	conn.commit()

def createDCTableLeveln(table, levels, numChunks, numCols, numRows):

	conn = mdb.connect(username="monetdb", password="monetdb", database="test")
	cur = conn.cursor()

	for c in range(numChunks):
		for i in range(1, 2**numCols):
			if(checkLevel1(i) == 1 or checkLevel2(i) == 1):
				#print("gotcha", i)
				continue
			
			vals = []
			for x in range(numCols):
				if((i << x) & 1 == 1):
					for y in range(i+1, numCols):
						if((i << y) & 1 == 1):
							cur.execute("SELECT col1 FROM dc_" + table + " WHERE col0 = " 
								+ idChunkCombine(2**i + 2**j, c, numChunks))
							
							vals.append(cur.fetchone()[0])	

			correlation = sum(vals) + 42

			cur.execute("INSERT INTO dc_" + table + " (col0, col1) VALUES (%s, %s)", 
				[idChunkCombine(i, c, numChunks), correlation])

	conn.commit()

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

def test():
	numChunks = 10
	numCols = 15
	numRows = 10000

	conn = mdb.connect(username="monetdb", password="monetdb", database="test")
	cur = conn.cursor()

	print(checkLevel2(9))


	createTable(cur, conn, "testa", numCols)
	insertRandData(cur, conn, "testa", numRows)
	conn.commit()

	createDCTableSetup("testa", numCols, numChunks, numCols, numRows)
	print("setup done")
	createDCTableLevel1("testa", numCols, numChunks, numCols, numRows)
	print("level 1 made")
	createDCTableLevel2("testa", numCols, numChunks, numCols, numRows)
	print("level 2 made")
	createDCTableLeveln("testa", numCols, numChunks, numCols, numRows)
	print("done")

	conn.commit()
	print(time.time() - startTime)

def exp():
	
	if(sys.argv[1] == "setup"):
		createDCTableSetup(sys.argv[2], int(sys.argv[3]),int( sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]))
	elif(sys.argv[1] == "level1"):
		createDCTableLevel1(sys.argv[2], int(sys.argv[3]),int( sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]))
	elif(sys.argv[1] == "level2"):
		createDCTableLevel2(sys.argv[2], int(sys.argv[3]),int( sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]))
	elif(sys.argv[1] == "leveln"):
		createDCTableLeveln(sys.argv[2], int(sys.argv[3]),int( sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]))

#if __name__=="__main__": startTime = time.time(); exp()
if __name__=="__main__": startTime = time.time(); test()




