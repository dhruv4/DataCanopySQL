import numpy as np

#for x in [10000, 100000, 1000000, 1000000, 100000000]:
for x in [100000000]:
	np.save("test" + str(x) + ".npy", np.random.rand(x, 15))
	print("made x")

