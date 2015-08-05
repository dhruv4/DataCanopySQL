import numpy as np

for x in range(2, 15+1):
#for x in [10000, 100000, 1000000, 10000000, 100000000]:
#for x in [5, 10, 15, 20, 25]:
	#np.save("test" + str(x) + ".npy", np.random.rand(x, 15))
	#np.savetxt("test" + str(x) + ".csv", np.random.randint(5, size=(x, 15)),delimiter=",",fmt='%i')
	np.savetxt("test" + str(x) + ".csv", np.random.randint(5, size=(100000, x)),fmt='%i', delimiter=",")
	print("made", x)

