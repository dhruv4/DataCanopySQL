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
#maxRows = math.ceil(numCols + math.log(numChunks, 2))
maxRows = (2**numCols - 1)*numChunks
sizeChunk = maxRows/numChunks

lng = False #unnecessary?
if(maxRows > 32):
	lng = True

def decToBinTrans(dec):

	print(int(bin(dec)[2:]))


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

	print(col, chunk, colBin + bin(chunk)[2:])
	return colBin + bin(chunk)[2:]

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
			cur.execute("SELECT AVG(ss) FROM (SELECT " 
				+ colList[i] + " AS ss FROM " 
				+ table + " LIMIT " + str(math.ceil(sizeChunk)) 
				+ " OFFSET " + str(x*sizeChunk) + ") as foo")
			avg = int(cur.fetchone()[0])
			cur.execute("INSERT INTO dc_" + table + " (col0, col1, col2, col3, col4, col5) VALUES (%s, %s, %s, %s, %s, %s)", [int(recToBinTrans([i], x), 2), avg, 0,0,0,0])

	print("reached")
	#cur.execute("SELECT banana FROM test LIMIT " + str(math.ceil(sizeChunk)) + "OFFSET 2")

	getAllData(cur, conn, "dc_" + table)
	print(cur.fetchall())

	#cur.execute("SELECT AVG(ss) as avg FROM (SELECT banana as ss FROM " + table + " LIMIT " + str(math.ceil(sizeChunk)) + " OFFSET 2) as foo")

	#cur.execute("SELECT * FROM (SELECT banana FROM test WHERE ROW_NUMBER() OVER() < 3) as foo")


	print(cur.fetchall())


def createTable(cur, conn, name, numCol, b=0):

	if(b == 1):
		cols = "(col0 varbit,"
		for x in range(1, numCol):
			cols += "col" + str(x) + " int,"
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

	if(sys.argv[2] == "get"):
		getAllData(cur, conn, sys.argv[3])
	elif(sys.argv[2] == "insert"):
		insertRandData(cur, conn, sys.argv[3], sys.argv[4])
	elif(sys.argv[2] == "graph"):
		graphData(cur, conn, sys.argv[3], sys.argv[4])
	elif(sys.argv[2] == "create"):
		createTable(cur, conn, sys.argv[3], sys.argv[4])


	createTable(cur, conn, "banana", numCols + 1, 1)
	#createTable(cur, conn, "test", numCols + 1)
	#insertRandData(cur, conn, "test", maxRows)
	#getAllData(cur, conn, "dc_test")
	#createDCTable(cur, conn, sys.argv[3])

	conn.commit()
	cur.close()
	conn.close()
	print("Run time: ", clock() - startTime, " seconds")

def test():

	print(decToBinTrans(3))

if __name__=="__main__": startTime = clock(); main()
#if __name__=="__main__": startTime = clock(); test()