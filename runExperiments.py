#runExperiments.py
import sys, os, testParts
import configparser

def main():
	db = sys.argv[1]

	numTrials = Config.getint("Experiment Config", "NumberOfTrials")

	numChunks = Config.getint("Data Canopy Config", "NumChunks")
	numRows = Config.getint("Data Set Config", "NumRows")
	numStats = Config.getint("Data Canopy Config", "NumStats")
	numCols = Config.getint("Data Set Config", "NumCols")
	numLevels = Config.getint("Data Canopy Config", "NumLevels")
	xaxis = Config.get("Experiment Config", "XAxis")


		for(level in ['setup', 'level1', 'level2', 'leveln'])	
			testParts.runExperiments(db, level, 2)

	print("Experiments Ran")