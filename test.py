import sys
import psycopg2 as pg
import monetdb.sql as mdb
import random
import math
from time import clock
from numpy import *
import Gnuplot, Gnuplot.funcutils

#DC INFO
K = 5 #num chunks
C = 5 #num cols
size = math.ceil(C + math.log(K, 2))
lng = False #unnecessary?
if(size > 32):
	lng = True

def binToDecTrans(bin):

	#input binary representation, return list of columns, chunk

	bin = str(bin)

	col = [i for i in range(C) if(bin[i] == '1')]
	chunk = int(bin[C:], 2)

	return col, chunk

def decToBinTrans(col, chunk):

	#input list of columns relevant, chunk number

	colBin = ""

	for x in range(C):
		if(x in col):
			colBin += '1'
		else:
			colBin += '0'


	return colBin + bin(chunk)[2:]

def createDCTable(cur, conn, table):

	createTable(cur, conn, 'dc_' + table, 6)
	'''
	-Get data chunk by chunk (see if you can get data row by row or rows by rows)
		-> OR get ALL data and then take chunks?
	-Calc stats for the chunks and rows
	-input stats into DC table with bin representation and stuff 
	'''

	


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
def insertRandData(cur, conn, table, length, typ):

	if(typ == "pg"):
		cur.execute("SELECT column_name from information_schema.columns where table_name='" + table + "'");
		colList = [x[0] for x in cur.fetchall()]
	else:
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

	conn.commit()
	cur.close()
	conn.close()
	print("Run time: ", clock() - startTime, " seconds")

def test():

	return

if __name__=="__main__": startTime = clock(); main()
#if __name__=="__main__": startTime = clock(); test()