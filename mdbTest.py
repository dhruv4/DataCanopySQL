#mdbTest.py
import sys
import monetdb.sql as mdb
import random
import math
from time import clock
from numpy import *
import Gnuplot, Gnuplot.funcutils

#DC INFO
numChunks = 5
numCols = 5
binLen = math.ceil(numCols + math.log(numChunks, 2))
maxRows = (2**numCols - 1)*numChunks
sizeChunk = math.ceil(maxRows/numChunks)
chunkBinLen = math.ceil(math.log(numChunks, 2))

lng = False #unnecessary?
if(maxRows > 32):
	lng = True

def decToBinTrans(dec):

	binCode = bin(dec)[2:]
	if(len(binCode) < binLen):
		lbc = len(binCode)
		for x in range(binLen - lbc):
			binCode = "0" + binCode


def binToRecTrans(bin):

	#input binary representation, return list of columns, chunk

	bin = str(bin)

	col = [i for i in range(numCols) if(bin[i] == '1')]
	chunk = int(bin[numCols:], 2)

	return col, chunk

def recToBinTrans(col, chunk):

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

	print(col, chunk, colBin + chunkBin)
	return colBin + chunkBin

def createDCTable(cur, conn, table):

	createTable(cur, conn, 'dc_' + table, 6)
	'''
	-Get data chunk by chunk (see if you can get data row by row or rows by rows)
	-Calc stats for the chunks
	-input stats into DC table with bin representation and stuff 
	'''

	cur.execute("SELECT * FROM " + table)
	colList = [x[0] for x in  cur.description]

	#works for one level of POSTGRES (I think) - idk how to do more levels yet
	for i in range(1, len(colList)):
		for x in range(numChunks):

			#cur.execute("CREATE FUNCTION GET_CHUNK(lim INT, off INT, tbl varchar(32), col varchar(32)) RETURNS TABLE (clm integer)"
			#	+" RETURN SELECT col FROM tbl LIMIT lim OFFSET off; END;")
			##^^This is the statement that SHOULD work but doesn't because monetdb doesn't recognize the variables like "col", "lim"
	
			cur.execute("CREATE FUNCTION GET_CHUNK() RETURNS TABLE (clm integer) "
				+"BEGIN RETURN SELECT " + colList[i] + " FROM " + table + " LIMIT " + str(sizeChunk) + " OFFSET " + str(x*sizeChunk) + "; END;")
			cur.execute("SELECT AVG(clm) FROM GET_CHUNK()")
			avg = int(cur.fetchone()[0])
			cur.execute("SELECT STDDEV_SAMP(clm) FROM GET_CHUNK()")
			std = int(cur.fetchone()[0])
			cur.execute("SELECT VAR_SAMP(clm) FROM GET_CHUNK()")
			var = int(cur.fetchone()[0])
			cur.execute("SELECT MEDIAN(clm) FROM GET_CHUNK()")
			med = int(cur.fetchone()[0])
			cur.execute("SELECT TOP 1 COUNT( ) val, freq FROM " + table + " GROUP BY " + colList[i] + " ORDER BY COUNT( ) DESC")
			mod = int(cur.fetchone()[0])
			cur.execute("INSERT INTO dc_" + table + " (col0, col1, col2, col3, col4, col5) VALUES (%s, %s, %s, %s, %s, %s)", [int(recToBinTrans([i], x), 2), avg, std,var,med,mod])
			cur.execute("DROP FUNCTION GET_CHUNK()")

	getAllData(cur, conn, "dc_" + table)

def createTable(cur, conn, name, numCol):

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
	g('set style data histograms')
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

		cur.execute(exe, [random.randint(0, 256) for x in range(len(colList))])


def getAllData(cur, conn, table):
	cur.execute("SELECT * FROM " + table)
	print(cur.fetchall())

def main():

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
	elif(sys.argv[2] == "createdc"):
		createDCTable(cur, conn, sys.argv[3])

	#createTable(cur, conn, "test", numCols + 1)
	#insertRandData(cur, conn, "test", maxRows)
	createDCTable(cur, conn, sys.argv[2])
	#getAllData(cur, conn, "dc_test")

	conn.commit()
	cur.close()
	conn.close()
	print("Run time: ", clock() - startTime, " seconds")

def test():

	print(decToBinTrans(3))

if __name__=="__main__": startTime = clock(); main()
#if __name__=="__main__": startTime = clock(); test()