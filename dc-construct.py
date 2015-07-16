#dc-contruct.py
import sys, random, math, itertools
import psycopg2 as pg
import time
from numpy import *
import monetdb.sql as mdb

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

def createTable(cur, conn, name, numCol, db, p=0, l=0):

	if(p == 1):
		if(db == "mdb"):
			if(l == 1):
				cols = "(col0 blob PRIMARY KEY,"
			else:
				cols = "(col0 bigint PRIMARY KEY,"
			for x in range(1, numCol):
				cols += "col" + str(x) + " double precision,"
		else:
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

def createDCTableSetup(table, levels, numChunks, numCols, numRows, db):

	if(db == "mdb"):
		conn = mdb.connect(username="monetdb", password="monetdb", database="test")
		cur = conn.cursor()
		if(numCols + math.ceil(math.log(numChunks, 2)) >= 32):
			createTable(cur, conn, 'dc_' + table, 6, db, 1, 1)
		else:
			createTable(cur, conn, 'dc_' + table, 6, db, 1)
	elif(db == "pg"):
		conn = pg.connect(dbname="postgres")
		cur = conn.cursor()
		createTable(cur, conn, 'dc_' + table, 6, db, 1)		

	conn.commit()

def createDCTableLevel1(table, levels, numChunks, numCols, numRows, db):

	if(db == "mdb"):
		conn = mdb.connect(username="monetdb", password="monetdb", database="test")
	elif(db == "pg"):
		conn = pg.connect(dbname="postgres")

	cur = conn.cursor()

	maxRows = (2**numCols - 1)*numChunks
	sizeChunk = math.floor(numRows/numChunks)
	#sizeChunk = math.ceil(numRows/numChunks)

	if(db == "mdb"):
		cur.execute("SELECT * FROM " + table)
		colList = [x[0] for x in  cur.description]
	elif(db == "pg"):
		cur.execute("SELECT column_name from information_schema.columns where table_name='" + table + "'");
		colList = [x[0] for x in cur.fetchall()]

	#level 1
	for i in range(1, len(colList)):
		for x in range(numChunks):

			if(db == "mdb"):
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
				cur.execute("DROP FUNCTION GET_CHUNK()")

			elif(db == "pg"):

				cur.execute("SELECT AVG(ss), STDDEV(ss), VAR_SAMP(ss) FROM (SELECT " 
				+ colList[j] + " AS ss FROM " 
				+ table + " LIMIT " + str(sizeChunk) 
				+ " OFFSET " + str(x*sizeChunk) + ") as foo")
				avg, stddev, var = cur.fetchone()

				med = 0 #median????

				#cur.execute("SELECT TOP 1 COUNT( ) val, freq FROM " + table + " GROUP BY " + colList[j] + " ORDER BY COUNT( ) DESC")
				#mod = int(cur.fetchone()[0])
				mod = 0

			if(db == "pg" or (db == "mdb" and (numCols + math.ceil(math.log(numChunks, 2)) >= 32))):
				cur.execute("INSERT INTO dc_" + table + " (col0, col1, col2, col3, col4, col5) VALUES (%s, %s, %s, %s, %s, %s)", 
					[recToBinTrans([i], x, numCols, numChunks), avg, std,var,med,mod])
			else:
				cur.execute("INSERT INTO dc_" + table + " (col0, col1, col2, col3, col4, col5) VALUES (%s, %s, %s, %s, %s, %s)", 
					[int(recToBinTrans([i], x, numCols, numChunks), 2), avg, std,var,med,mod])
			
	conn.commit()
	
def createDCTableLevel2(table, levels, numChunks, numCols, numRows, db):

	if(db == "mdb"):
		conn = mdb.connect(username="monetdb", password="monetdb", database="test")
	elif(db == "pg"):
		conn = pg.connect(dbname="postgres")

	cur = conn.cursor()

	maxRows = (2**numCols - 1)*numChunks
	sizeChunk = math.floor(numRows/numChunks)
	#sizeChunk = math.ceil(numRows/numChunks)

	if(db == "mdb"):
		cur.execute("SELECT * FROM " + table)
		colList = [x[0] for x in  cur.description]
	elif(db == "pg"):
		cur.execute("SELECT column_name from information_schema.columns where table_name='" + table + "'");
		colList = [x[0] for x in cur.fetchall()]

	print("reached 2")

	#level 2
	for i, j in itertools.combinations(range(1, numCols+1), 2):
		for c in range(numChunks):
			
			if(db == "mdb"):
				cur.execute("CREATE FUNCTION GET_CHUNK() RETURNS TABLE (cl1 bigint, cl2 bigint) "
				+ "BEGIN RETURN SELECT " + colList[i] + "," + colList[j] + " FROM " + table 
				+ " LIMIT " + str(sizeChunk) + " OFFSET " + str(c*sizeChunk) + "; END;")
				
				cur.execute("SELECT CORR(cl1, cl2) FROM GET_CHUNK()")

			elif(db == "pg"):
				cur.execute("SELECT CORR(x, y) FROM (SELECT cast(" + colList[j] + " as double precision) AS x, cast(" 
					+ colList[i] + " as double precision) AS y FROM " 
					+ table + " LIMIT " + str(sizeChunk) 
					+ " OFFSET " + str(c*sizeChunk) + ") as foo")

			if(db == "pg" or (db == "mdb" and (numCols + math.ceil(math.log(numChunks, 2)) >= 32))):
				cur.execute("INSERT INTO dc_" + table + " (col0, col1) VALUES (%s, %s)", 
					[recToBinTrans([i, j], c, numCols, numChunks), float(cur.fetchone()[0])])
			else:
				cur.execute("INSERT INTO dc_" + table + " (col0, col1) VALUES (%s, %s)", 
					[int(recToBinTrans([i, j], c, numCols, numChunks), 2), cur.fetchone()[0]])
				
			cur.execute("DROP FUNCTION GET_CHUNK()")

	conn.commit()

def createDCTableLeveln(table, levels, numChunks, numCols, numRows, db, two = 0):

	if(db == "mdb"):
		conn = mdb.connect(username="monetdb", password="monetdb", database="test")
	elif(db == "pg"):
		conn = pg.connect(dbname="postgres")

	cur = conn.cursor()

	#3-n Levels
	for i in range(3, levels+1):
		print("reached", i)
		comb = list(itertools.combinations(range(1, numCols + 1), i))
		for cval in range(numChunks):
			for j in comb:
				vals = []
				if(two == 1):
					comb2 = list(itertools.combinations(j, 2))
				else:
					comb2 = list(itertools.combinations(j, i-1))
				for k in comb2:
					
					if(db == "pg"):
						cur.execute("SELECT col1 FROM dc_" + table + " WHERE col0 = cast('" 
							+ recToBinTrans(k, cval, numCols, numChunks) + "' as varbit)")
					
					elif(db == "mdb"):
						if(numCols + math.ceil(math.log(numChunks, 2)) >= 32):
							cur.execute("SELECT col1 FROM dc_" + table + " WHERE col0='" 
								+ recToBinTrans(k, c, numCols, numChunks) + "'")
						else:
							cur.execute("SELECT col1 FROM dc_" + table + " WHERE col0='" 
								+ str(int(recToBinTrans(k, c, numCols, numChunks), 2)) + "'")

					vals.append(cur.fetchone()[0])				

				correlation = sum(vals) + 42

				if(db == "pg"):
					cur.execute("INSERT INTO dc_" + table + " (col0, col1) VALUES (%s, %s)", 
						[recToBinTrans(j, cval, numCols, numChunks), correlation])

				elif(db == "mdb"):
					if(numCols + math.ceil(math.log(numChunks, 2)) >= 32):
						cur.execute("INSERT INTO dc_" + table + " (col0, col1) VALUES (%s, %s)", 
							[str(recToBinTrans(j, c, numCols, numChunks)), correlation])
					else:
						cur.execute("INSERT INTO dc_" + table + " (col0, col1) VALUES (%s, %s)", 
							[str(int(recToBinTrans(j, c, numCols, numChunks), 2)), correlation])
			conn.commit()