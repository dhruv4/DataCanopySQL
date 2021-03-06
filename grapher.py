#grapher.py

import matplotlib.pyplot as plt
import sys, os

def graph(xtitle, name, level, db, c=0, ylog=0):

	if not os.path.exists("pgresults/graphs"):
		os.makedirs("pgresults/graphs")

	if not os.path.exists("mdbresults/graphs"):
		os.makedirs("mdbresults/graphs")

	if(c == 1):
		f = open(db + 'results/' + name + level + '_' + db + '_cache_' + xtitle +  '.txt', 'r')
	else:
		f = open(db + 'results/' + name + level + '_' + db + '_time_' + xtitle +  '.txt', 'r')

	x = []
	t = []

	for line in f:
		nums = line.split(',')
		x.append(nums[0])
		t.append(nums[1][:-1])

	print("x", x, "t", t, name, level)

	if(level =="setup"):
		plt.plot(x, t, '-yo', label=level)
	if(level =="level1"):
		plt.plot(x, t, '-r+', label="Level 1")
	if(level =="level2"):
		plt.plot(x, t, '-gx', label="Level 2")
	if(level =="leveln"):
		plt.plot(x, t, '-b*', label="Rest of Levels")
	if(level =="total"):
		plt.plot(x, t, '-ms', label="Total")

	plt.legend(loc="best", frameon=False)

	if(xtitle == "Rows"):
		plt.xscale('log')

	if(c == 1):	
		if(xtitle =="Rows"):
			plt.title("Varying Number of Rows vs Cache Misses")
			plt.xlabel("Number of Rows")
		if(xtitle =="cols"):
			plt.title("Varying Number of Columns vs Cache Misses")
			plt.xlabel("Number of Columns")
		if(xtitle =="chunks"):
			plt.title("Varying Number of Chunks vs Cache Misses")
			plt.xlabel("Number of Chunks")

		plt.ylabel('Cache Misses')

	else:
		if(xtitle =="Rows"):
			plt.title("Varying Number of Rows vs Time (sec)")
			plt.xlabel("Number of Rows")
		if(xtitle =="cols"):
			plt.title("Varying Number of Columns vs Time (sec)")
			plt.xlabel("Number of Columns")
		if(xtitle =="chunks"):
			plt.title("Varying Number of Chunks vs Time (sec)")
			plt.xlabel("Number of Chunks")

		plt.ylabel('Time (Sec)')

	plt.tight_layout()
	
	if(ylog == 1):
		if(c == 1):
			plt.yscale('log')
			plt.savefig(db + 'results/graphs/' + db + '_cache_' + name + xtitle + 'log.pdf')
		else:
			plt.yscale('log')
			plt.savefig(db + 'results/graphs/' + db + '_time_' + name + xtitle + 'log.pdf')
	else:
		if(c == 1):
			plt.savefig(db + 'results/graphs/' + db + '_cache_' + name + xtitle + '.pdf')
		else:
			plt.savefig(db + 'results/graphs/' + db + '_time_' + name + xtitle + '.pdf')


def main():

	#arg1 = db
	#arg2 = Test, Test2
	#arg3 = Rows, Cols, Chunks

	#levels = ['setup', 'level1', 'level2', 'leveln', 'total']
	levels = ['level1', 'level2', 'leveln', 'total']

	for j in levels:

		graph(sys.argv[3], sys.argv[2], j, sys.argv[1], 1)

	print("graphed cache")

	plt.close()

	for j in levels:

		graph(sys.argv[3], sys.argv[2], j, sys.argv[1], 1, 1)
	
	print("graphed cache with log")

	plt.close()

	for j in levels:

		graph(sys.argv[3], sys.argv[2], j, sys.argv[1], 0)

	print("graphed time")

	plt.close()

	for j in levels:

		graph(sys.argv[3], sys.argv[2], j, sys.argv[1], 0, 1)
	
	print("graphed time with log")

	plt.close()


def cgraph(xtitle, name, db, c=0, ylog=0):

	if not os.path.exists("results/graphs"):
		os.makedirs("results/graphs")

	if(c == 1):
		f = open(db + 'results/' + name + 'total' + '_' + db + '_cache_' + xtitle +  '.txt', 'r')
	else:
		f = open(db + 'results/' + name + 'total' + '_' + db + '_time_' + xtitle +  '.txt', 'r')

	x = []
	t = []

	for line in f:
		nums = line.split(',')
		x.append(nums[0])
		t.append(nums[1][:-1])

	print("x", x, "t", t, name, db)

	if(db =="dc"):
		plt.plot(x, t, '-yo', label="Data Canopy System")
	if(db =="pg"):
		plt.plot(x, t, '-r+', label="Postgres")
	if(db =="mdb"):
		plt.plot(x, t, '-gx', label="MonetDB")

	plt.legend(loc="best", frameon=False)
	if(xtitle == "Rows" or xtitle == "Chunks"):
		plt.xscale('log')

	if(c == 1):	
		if(xtitle =="Rows"):
			plt.title("Varying Number of Rows vs Cache Misses")
			plt.xlabel("Number of Rows")
		if(xtitle =="Cols"):
			plt.title("Varying Number of Columns vs Cache Misses")
			plt.xlabel("Number of Columns")
		if(xtitle =="Chunks"):
			plt.title("Varying Number of Chunks vs Cache Misses")
			plt.xlabel("Number of Chunks")

		plt.ylabel('Cache Misses')
		
	else:
		if(xtitle =="Rows"):
			plt.title("Varying Number of Rows vs Time (sec)")
			plt.xlabel("Number of Rows")
		if(xtitle =="Cols"):
			plt.title("Varying Number of Columns vs Time (sec)")
			plt.xlabel("Number of Columns")
		if(xtitle =="Chunks"):
			plt.title("Varying Number of Chunks vs Time (sec)")
			plt.xlabel("Number of Chunks")

		plt.ylabel('Time (Sec)')

	plt.tight_layout()
	
	if(ylog == 1):
		if(c == 1):
			plt.yscale('log')
			plt.savefig('results/graphs/compare_cache_' + name + xtitle + 'log.pdf')
		else:
			plt.yscale('log')
			plt.savefig('results/graphs/compare_time_' + name + xtitle + 'log.pdf')
	else:
		if(c == 1):
			plt.savefig('results/graphs/compare_cache_' + name + xtitle + '.pdf')
		else:
			plt.savefig('results/graphs/compare_time_' + name + xtitle + '.pdf')

def compare():

	#arg1 = Test, Test2, TestNew
	#arg2 = Rows, Cols, Chunks

	levels = ['dc', 'pg', 'mdb']

	for j in levels:

		cgraph(sys.argv[2], sys.argv[1], j, 1)

	print("graphed cache")

	plt.close()

	for j in levels:

		cgraph(sys.argv[2], sys.argv[1], j, 1, 1)
	
	print("graphed cache with log")

	plt.close()

	for j in levels:

		cgraph(sys.argv[2], sys.argv[1], j, 0)

	print("graphed time")

	plt.close()

	for j in levels:

		cgraph(sys.argv[2], sys.argv[1], j, 0, 1)
	
	print("graphed time with log")

	plt.close()

#main()
compare()


