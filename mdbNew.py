#mdbNew.py
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
	while (((x & 1) == 0) and x > 1): #While x is even and > 1
		x >>= 1
	return (x == 2)
	'''
	return bin(x).count('1') == 2

def createTable(cur, conn, name, numCol, b=0, l=0):

	if(b == 1):
		
		if(l == 1):
			cols = "(col0 bigint PRIMARY KEY,"
		else:
			cols = "(col0 int PRIMARY KEY,"

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

	if(numCols + math.ceil(math.log(numChunks, 2)) >= 32):
		createTable(cur, conn, 'dc_' + table, 6, 1, 1)
	else:
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

			#CREATE FUNCTION fWedgeV4(x1 float, y1 float, z1 float, x2 float, y2 int, z2 int) RETURNS TABLE (x float, y float, z float) RETURN TABLE (SELECT (y2) as x, x2 as y, z2 as z LIMIT y2 OFFSET z2 );

			#cur.execute("CREATE FUNCTION BS_STUFF( s1 varchar(32), st int, len int, s3 varchar(32)) RETURNS varchar(32) BEGIN DECLARE res varchar(32), aux varchar(32);  DECLARE ofset int; SET ofset = 0; RETURN res; END;")

			#cur.execute("CREATE FUNCTION GET_CHUNK(lim int, off int, tbl varchar(32), col varchar(32)) RETURNS TABLE (clm int) BEGIN RETURN PREPARE SELECT col FROM tbl LIMIT ? OFFSET ?; END;")

			#cur.execute("CREATE FUNCTION GET_BANANA(lim int, off int, tbl varchar(32), col varchar(32)) RETURNS TABLE (clm int) BEGIN"
			#+  "DECLARE stmt1 varchar(255); DECLARE stmt2 varchar(255); DECLARE stmt3 varchar(255); DECLARE stmt4 varchar(255); DECLARE stmt5 varchar(255); DECLARE stmt6 varchar(255); DECLARE stmt7 varchar(255);" 
			#+ " SET stmt1 = concat('SELECT', col); SET stmt2 = concat('FROM', tbl); SET stmt3 = concat('LIMIT', lim); SET stmt4 = concat('OFFSET', off); SET stmt5 = concat(@stmt1, @stmt2); SET stmt6 = concat(@stmt3, @stmt4); SET stmt7 = concat(@stmt5, @stmt6);"
			#+ " PREPARE stmt FROM @stmt7; EXECUTE stmt; RETURN col; END;")

			#cur.execute("CREATE FUNCTION GET_CHUNK(lim int, off int, tbl varchar(32), col varchar(32)) RETURNS TABLE (clm int) BEGIN PREPARE SELECT col FROM tbl LIMIT ? OFFSET ?; RETURN EXEC (lim, off); END;")

CREATE FUNCTION GET_BANANA(lim int, off int, tbl varchar(32), col varchar(32)) RETURNS TABLE (clm int) BEGIN PREPARE SELECT col FROM tbl LIMIT ? OFFSET ?; RETURN EXEC (lim, off); END;

			#cur.execute("CREATE PROCEDURE GET_CHUNK(lim int, off int, tbl varchar(32), col varchar(32)) BEGIN SELECT col FROM tbl LIMIT lim OFFSET off; END;")

			##^^This is the statement that SHOULD work but doesn't because monetdb doesn't recognize the variables like "col", "lim"
			
			#cur.execute("CREATE FUNCTION GET_CHUNK() RETURNS TABLE (clm integer) "
			#	+"BEGIN RETURN SELECT " + colList[i] + " FROM " + table + " LIMIT " + str(sizeChunk) + " OFFSET " + str(c*sizeChunk) + "; END;")
			
			#cur.execute("SELECT AVG(clm), STDDEV_SAMP(clm), VAR_SAMP(clm), MEDIAN(clm) FROM GET_CHUNK()")

			#removed median for consistency

			#cur.execute("SELECT AVG(clm), STDDEV_SAMP(clm), VAR_SAMP(clm) FROM GET_CHUNK()")

			cur.execute("SELECT AVG(col), STDDEV_SAMP(col), VAR_SAMP(col) FROM GET_CHUNK(" + str(sizeChunk) + ", " + str(c*sizeChunk) + ", '" + table + "', '" + colList [i] + "')")

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
				if((i >> x) & 1 == 1):
					for y in range(x+1, numCols):
						if((i >> y) & 1 == 1):
							cur.execute("SELECT col1 FROM dc_" + table + " WHERE col0 = " 
								+ str(idChunkCombine(2**x + 2**y, c, numChunks)))
							
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
	numCols = 5
	numRows = 1000

	conn = mdb.connect(username="monetdb", password="monetdb", database="test")
	cur = conn.cursor()

	print(checkLevel2(9))

	'''
	createTable(cur, conn, "testa", numCols)
	insertRandData(cur, conn, "testa", numRows)
	conn.commit()
	'''
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




