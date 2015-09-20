#include <stdio.h>                                                                                                           
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <mapi.h>

typedef enum { false, true } bool;

bool checkLevel1(long long x){

	return __builtin_popcount(x)==1;
}

bool checkLevel2(long long x){

	return __builtin_popcount(x)==2;
		
}
void die(Mapi dbh, MapiHdl hdl)
{
	if (hdl != NULL) {
		mapi_explain_query(hdl, stderr);
		do {
			if (mapi_result_error(hdl) != NULL)
				mapi_explain_result(hdl, stderr);
		} while (mapi_next_result(hdl) == 1);
		mapi_close_handle(hdl);
		mapi_destroy(dbh);
	} else if (dbh != NULL) {
		mapi_explain(dbh, stderr);
		mapi_destroy(dbh);
	} else {
		fprintf(stderr, "command failed\n");
	}
	exit(-1);
}

MapiHdl query(Mapi dbh, char *q)
{
	MapiHdl ret = NULL;
	if ((ret = mapi_query(dbh, q)) == NULL || mapi_error(dbh) != MOK)
		die(dbh, ret);
	return(ret);
}

void update(Mapi dbh, char *q)
{
	MapiHdl ret = query(dbh, q);
	if (mapi_close_handle(ret) != MOK){
		die(dbh, ret);
	}
}

void createTable(Mapi dbh, char* name, int numCol, bool b, bool l){
	
	char cols[255] = "(", comd[255] = "CREATE TABLE ", dbl[255] = "%d double precision,", itgr[255] = "%d int,";
	int x;

	if(b){
		if(l){
			strcat(cols,"col0 bigint PRIMARY KEY,");
		} else {
			strcat(cols,"col0 int PRIMARY KEY,");
		}
		for(x = 1; x < numCol; x++){
			strcat(cols, "col");
			sprintf(dbl, dbl, x);
			strcat(cols, dbl);
			strcpy(dbl, "%d double precision,");
		}

	} else {
		for(x = 0; x < numCol; x++){
			strcat(cols, " col");
			sprintf(itgr, itgr, x);
			strcat(cols, itgr);
			strcpy(itgr, "%d int,");
		}
	}

	cols[strlen(cols)-1] = 0;

	strcat(cols, ")");

	strcat(comd, name);

	strcat(comd, cols);

	update(dbh, comd);

}
/*
void insertRandData(PGconn* conn, char* table, int length){

	cur.execute("SELECT column_name from information_schema.columns where table_name='" + table + "'")
	colList = [x[0] for x in cur.fetchall()]

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

		cur.execute(exe, [random.randint(0, 5) for x in range(len(colList))])
}
*/

int idChunkCombine(long long idn, int chunk, int numChunks){
	return ((idn << (int)ceil((log(numChunks)/log(2)))) | chunk);
}

void createDCTableSetup(char* table, int levels, int numChunks, int numCols, int numRows){
	
	Mapi dbh;
	dbh = mapi_connect("dhruv-VirtualBox", 50000,"monetdb", "monetdb", "sql", "test");

	char dc[255];

	sprintf(dc, "dc_%s", table);

	if (numCols + (int)ceil(log(numChunks)/log(2)) >= 32){
		createTable(dbh, dc, 6, true, true);
	} else {

		createTable(dbh, dc, 6, true, false);
	}

	mapi_disconnect(dbh);
}

int createDCTableLevel1(char* table, int levels, int numChunks, int numCols, int numRows){

	Mapi dbh;
	dbh = mapi_connect("dhruv-VirtualBox", 50000,"monetdb", "monetdb", "sql", "test");
	MapiHdl hdl = NULL;

	char colcmd[255];

	sprintf(colcmd, "SELECT name FROM sys._columns WHERE table_id = (SELECT id FROM sys._tables WHERE name='%s')", table);

	hdl = query(dbh, colcmd);

	char colList[numCols][256];

	int i = 0;

	while (mapi_fetch_row(hdl)) {
		strcpy(colList[i], mapi_fetch_field(hdl, 0));
		i++;
	}

	int maxRows = (pow(2,numCols) - 1)*numChunks;
	int sizeChunk = (int)ceil(numRows/numChunks);

	char statcmd[255], inscmd[255];
	double avg, std, var, med, mod;
	int ID, c;
	for(c = 0; c < numChunks; c++){
		for(i = 0; i < numCols; i++){
			
			sprintf(statcmd, "SELECT AVG(%s), STDDEV_SAMP(%s), VAR_SAMP(%s) FROM (SELECT %s, ROW_NUMBER() OVER() as rnum FROM %s) AS foo WHERE rnum > %d AND rnum < %d", colList[i], colList[i], colList[i], colList[i], table, c*sizeChunk, sizeChunk + c*sizeChunk);

			hdl = query(dbh, statcmd);

			mapi_fetch_row(hdl);

			avg = atof(mapi_fetch_field(hdl, 0));
			std = atof(mapi_fetch_field(hdl, 1));
			var = atof(mapi_fetch_field(hdl, 2));

			//printf("%f %f %f\n", avg, std, var);

			med = 0; //median

			//cur.execute("SELECT TOP 1 COUNT( ) val, freq FROM " + table + " GROUP BY " + colList[j] + " ORDER BY COUNT( ) DESC")
			//mod = int(cur.fetchone()[0])
			
			mod = 0;

			ID = 1<<i;

			ID = idChunkCombine(ID, c, numChunks);

			sprintf(inscmd, "INSERT INTO dc_%s (col0, col1, col2, col3, col4, col5) VALUES (%d, %f, %f, %f, %f, %f)", table, ID, avg, std, var, med, mod);

			update(dbh, inscmd);

		}
	}
	mapi_close_handle(hdl);
	mapi_disconnect(dbh);
}
int createDCTableLevel2(char* table, int levels, int numChunks, int numCols, int numRows){
	
	Mapi dbh;
	dbh = mapi_connect("dhruv-VirtualBox", 50000,"monetdb", "monetdb", "sql", "test");
	MapiHdl hdl = NULL;

	char colcmd[255];

	sprintf(colcmd, "SELECT name FROM sys._columns WHERE table_id = (SELECT id FROM sys._tables WHERE name='%s')", table);

	hdl = query(dbh, colcmd);

	char colList[numCols][256];

	int c, i = 0, j;

	while (mapi_fetch_row(hdl)) {
		strcpy(colList[i], mapi_fetch_field(hdl, 0));
		i++;
	}

	char statcmd[255], inscmd[255];

	int maxRows = (pow(2,numCols) - 1)*numChunks;
	int sizeChunk = (int)ceil(numRows/numChunks);

	float corr;

	for(c = 0; c < numChunks; c++){
		for(i = 0; i < numCols - 1; i++){
			for(j = i+1; j < numCols; j++){

				sprintf(statcmd, "SELECT CORR(cl1, cl2) FROM (SELECT cast(%s as double precision) AS cl1, cast(%s as double precision) AS cl2, ROW_NUMBER() OVER() as rnum FROM %s) as foo WHERE rnum > %d AND rnum < %d", 
					colList[i], colList[j], table, c*sizeChunk, sizeChunk + c*sizeChunk);

				hdl = query(dbh, statcmd);

				mapi_fetch_row(hdl);

				corr = atof(mapi_fetch_field(hdl, 0));

				sprintf(inscmd, "INSERT INTO dc_%s (col0, col1) VALUES (%d, %f)", table, idChunkCombine((long long)(pow(2,i) + pow(2,j)), c, numChunks), corr);

				update(dbh, inscmd);
			}
		}
	}
	mapi_close_handle(hdl);
	mapi_disconnect(dbh);
}
int createDCTableLeveln(char* table, int levels, int numChunks, int numCols, int numRows){

	Mapi dbh;
	dbh = mapi_connect("dhruv-VirtualBox", 50000,"monetdb", "monetdb", "sql", "test");
	MapiHdl hdl = NULL;
	
	int c, x, y;

	long long i;

	long long numColPow = (long long)pow(2, numCols);

	double correlation;

	float vals[numCols*numCols];

	int counter;
	int bananact = 0;
	char statcmd[255], inscmd[255];

	for(c = 0; c < numChunks; c++){
		for(i = 1; i < numColPow; i++){

			if(checkLevel1(i) || checkLevel2(i)){
				continue;
			}
			
			memset(vals, 0, sizeof(int)*(numCols*numCols));

			for(x = 0; x < numCols; x++){

				if((i >> x) & 1 == 1)
					for(y = x+1; y < numCols; y++){

						if((i >> y) & 1 == 1){
							sprintf(statcmd, "SELECT col1 FROM dc_%s WHERE col0 = %d", 
								table, idChunkCombine((long long)(pow(2,x) + pow(2,y)), c, numChunks));

							hdl = query(dbh, statcmd);
							counter++;

							mapi_fetch_row(hdl);

							vals[counter] = atof(mapi_fetch_field(hdl, 0));

						}
					}
			}
			correlation = 42.0;

			counter = 0;

			sprintf(inscmd, "INSERT INTO dc_%s (col0, col1) VALUES (%d, %f)", table, idChunkCombine(i, c, numChunks), correlation);
				
			update(dbh, inscmd);

		}
	}
	mapi_close_handle(hdl);
	mapi_disconnect(dbh);
}
int main( int argc, char* argv[]){

	//rewrite to fit experiments
	//ARGS: setup exp numLevels numChunks numCols numRows

	int numLevels, numRows, numChunks, numCols;
	char tableName[256], command[256];

	if(argc > 1){
		strcpy(command, argv[1]);
		strcpy(tableName, argv[2]);
		numLevels = atoi(argv[3]);
		numRows = atoi(argv[4]);
		numChunks = atoi(argv[5]);
		numCols = atoi(argv[6]);
	} else {
		strcpy(command, "all");
		strcpy(tableName, "banana");
		numRows = 1000;
		numChunks = 10;
		numCols = 10;
		numLevels = numCols;

	}
	/*
	int x;
	for(x = 0; x < argc; x++){
		printf("%s\n", argv[x]);
	}

	time_t curtime;
    struct timeval tv;
    gettimeofday(&tv, NULL); 
    curtime=tv.tv_sec;

	clock_t start = clock(), diff;
	*/

	if(command == "setup" || command == "all")
		createDCTableSetup(tableName, numCols, numChunks, numCols, numRows);
	if(command == "level1" || command == "all")
		createDCTableLevel1(tableName, numCols, numChunks, numCols, numRows);
	if(command == "level2" || command == "all")
		createDCTableLevel2(tableName, numCols, numChunks, numCols, numRows);
	if(command == "leveln" || command == "all")
		createDCTableLeveln(tableName, numCols, numChunks, numCols, numRows);
	
	/*
	diff = clock() - start;

	int msec = diff * 1000 / CLOCKS_PER_SEC;
	printf("CPU Time taken %d seconds %d milliseconds\n", msec/1000, msec%1000);
	gettimeofday(&tv, NULL);
	printf("Elapsed Time %ld seconds\n", tv.tv_sec - curtime);
	*/

	return 0;
}