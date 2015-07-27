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

	print("x", x, "t", t, name)

	if(name[4:]=="setup"):
		plt.plot(x, t, '-yo', label=name[4:])
	if(name[4:]=="level1"):
		plt.plot(x, t, '-r+', label=name[4:])
	if(name[4:]=="level2"):
		plt.plot(x, t, '-gx', label=name[4:])
	if(name[4:]=="leveln"):
		plt.plot(x, t, '-b*', label=name[4:])
	if(name[4:]=="total"):
		plt.plot(x, t, '-ms', label=name[4:])

	plt.legend(loc="best")
	if(xtitle == "Rows"):
		plt.xscale('log')

	if(c == 1):	
		plt.title(xtitle + " vs Cache Misses")
		plt.ylabel('Cache Misses')
	else:
		plt.title(xtitle + " vs Time (sec)")
		plt.ylabel('Time (Sec)')
	plt.tight_layout()
	plt.xlabel(xtitle)
	
	if(ylog == 1):
		if(c == 1):
			plt.yscale('log')
			plt.savefig(db + 'results/graphs/mpl_' + db + '_cache_' + name + xtitle + 'log.pdf')
		else:
			plt.yscale('log')
			plt.savefig(db + 'results/graphs/mpl_' + db + '_time_' + name + xtitle + 'log.pdf')
	else:
		if(c == 1):
			plt.savefig(db + 'results/graphs/mpl_' + db + '_cache_' + name + xtitle + '.pdf')
		else:
			plt.savefig(db + 'results/graphs/mpl_' + db + '_time_' + name + xtitle + '.pdf')


def main():

	#arg1 = db
	#arg2 = Test, Test2
	#arg3 = Rows, Cols, Chunks

	levels = ['setup', 'level1', 'level2', 'leveln', 'total']

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


#plt = matplotlib.pyplot

main()
