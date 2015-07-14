# DataCanopySQL

Updated: 6/26/15

> * Still debugging
> * Still working on refactoring

## Basic Usage
1. Use Python to run program for the SQL Database of your choice
2. When running, use the arguments to select what you want to do
     * For example: `python mdbTest.py create test_tbl 150`
3. Enjoy!

##Method Descriptions

* getAllData(cursor, connection, table)
     * Gets all data in table
     * Use `get YOUR_TABLE_NAME` as arguments to acccess this method
* insertRandData(cursor, connection, tableName, length)
     * Inserts randomly generated integers into table with a specified number of rows
     * Assumes all columns in table are integers
     * Use `insert YOUR_TABLE_NAME NUMBER_OF_ROWS`
* graphData(cursor, connection, tableName, column)
     * _Work in Progress_
     * Graphs data from specified column in specified table
     * Use `graph YOUR_TABLE_NAME YOUR_COLUMN_NAME`
* createTable(cursor, connection, tableName, numberColumns)
     * Creates table with specified name and specified number of columns
     * Use `create YOUR_TABLE_NAME YOUR_NUMBER_COLUMNS`
* createDCTable(cursor, connection, tableName)
     * Creates a Data Canopy Table called `dc_YOUR_TABLE_NAME` for a specified table
     * Assumes all data is integers
     * Assumes first column is used for identification
     * Use `createdc YOUR_TABLE_NAME`

##Experiment Instructions

Edit Config.ini to change the information about the tests such as variable values and which variable to vary.

###Time Tests

Run program as python3 test.py [database - pg or mdb]

###Cache Tests

Run program as python3 test.py [database - pg or mdb]

Dhruv Gupta - [Harvard SEAS DASLab](//daslab.seas.harvard.edu) - [dgupta@g.harvard.edu](mailto:dgupta@g.harvard.edu)
     
