#include <stdio.h>                                                                                                           
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <libpq-fe.h>

typedef enum { false, true } bool;

bool checkLevel1(long long x){

	return __builtin_popcount(x)==1;
}

bool checkLevel2(long long x){

	return __builtin_popcount(x)==2;
		
}
void createTable(PGconn * conn, char* name, int numCol, bool b, bool l){
	
	char cols[255] = "(", comd[255] = "CREATE TABLE ", dbl[255] = "%d double precision,", itgr[255] = "%d int,";
	int x;
	PGresult * result;

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
			strcat(cols, "col");
			sprintf(itgr, itgr, x);
			strcat(cols, itgr);
			strcpy(itgr, "%d int,");
		}
	}

	cols[strlen(cols)-1] = 0;

	strcat(cols, ")");

	strcat(comd, name);

	strcat(comd, cols);

	if((result = PQexec( conn, comd )) == NULL){
		printf("well that was null\n" );
	}

	if (PQresultStatus(result) != PGRES_TUPLES_OK && strlen(PQerrorMessage(conn)) > 2)
    {
        printf("CREATE TABLE failed: %s\n", PQerrorMessage(conn));
        PQclear(result);
        //exit(0);
    } else {
		PQclear(result);
	}
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
	
	PGconn * conn;
	conn = PQconnectdb( "dbname=postgres" );

	char dc[255];

	sprintf(dc, "dc_%s", table);

	if (numCols + (int)ceil(log(numChunks)/log(2)) >= 32){
		createTable(conn, dc, 6, true, true);
	} else {

		createTable(conn, dc, 6, true, false);
	}

	PQfinish(conn);
}
int createDCTableLevel1(char* table, int levels, int numChunks, int numCols, int numRows){

	PGconn * conn;
	conn = PQconnectdb( "dbname=postgres" );

	PGresult * result;

	char colcmd[255];

	sprintf(colcmd, "SELECT column_name from information_schema.columns where table_name='%s'", table);

	if(( result = PQexec( conn, colcmd )) == NULL && strlen(PQerrorMessage(conn)) > 2)   {
	    printf( "%s\n", PQerrorMessage( conn ));
	}

	char colList[numCols][256];

	int i;

	for (i = 0; i < numCols; ++i)
	{
		strcpy(colList[i], PQgetvalue( result, i, 0 ));
	}

	PQclear(result);

	int maxRows = (pow(2,numCols) - 1)*numChunks;
	int sizeChunk = (int)ceil(numRows/numChunks);

	char statcmd[255], inscmd[255];
	double avg, std, var, med, mod;
	int ID, c;
	for(c = 0; c < numChunks; c++){
		for(i = 0; i < numCols; i++){
			
			sprintf(statcmd, "SELECT AVG(ss), STDDEV(ss), VAR_SAMP(ss) FROM (SELECT %s AS ss FROM %s LIMIT %d OFFSET %d) as foo", colList[i], table, sizeChunk, c*sizeChunk);

			if(( result = PQexec( conn, statcmd )) == NULL && strlen(PQerrorMessage(conn)) > 2)   {
				printf( "%s\n", PQerrorMessage( conn ));
			}

			avg = atof(PQgetvalue(result, 0, 0));
			std = atof(PQgetvalue(result, 0, 1));
			var = atof(PQgetvalue(result, 0, 2));

			//printf("%f %f %f\n", avg, std, var);

			PQclear(result);

			med = 0; //median

			//cur.execute("SELECT TOP 1 COUNT( ) val, freq FROM " + table + " GROUP BY " + colList[j] + " ORDER BY COUNT( ) DESC")
			//mod = int(cur.fetchone()[0])
			
			mod = 0;

			ID = 1<<i;

			ID = idChunkCombine(ID, c, numChunks);

			sprintf(inscmd, "INSERT INTO dc_%s (col0, col1, col2, col3, col4, col5) VALUES (%d, %f, %f, %f, %f, %f)", table, ID, avg, std, var, med, mod);

			if((result = PQexec( conn, inscmd )) == NULL){
				printf("well that was null\n" );
			}

			if (PQresultStatus(result) != PGRES_TUPLES_OK && strlen(PQerrorMessage(conn)) > 2)
		    {
		        printf("INSERT failed: %s\n", PQerrorMessage(conn));
		        PQclear(result);
		        //exit(0);
		    } else {
		    	PQclear(result);
		    }

		}
	}
	PQfinish(conn);
}

int createDCTableLevel2(char* table, int levels, int numChunks, int numCols, int numRows){
	
	PGconn * conn;
	conn = PQconnectdb( "dbname=postgres" );

	PGresult * result;

	char colcmd[255];

	sprintf(colcmd, "SELECT column_name from information_schema.columns where table_name='%s'", table);

	if(( result = PQexec( conn, colcmd )) == NULL && strlen(PQerrorMessage(conn)) > 2)   {
	    printf( "%s\n", PQerrorMessage( conn ));
	}

	char colList[numCols][256];

	int c, i, j;

	for (i = 0; i < numCols; ++i)
	{
		strcpy(colList[i], PQgetvalue( result, i, 0 ));
	}

	PQclear(result);

	char statcmd[255], inscmd[255];

	int maxRows = (pow(2,numCols) - 1)*numChunks;
	int sizeChunk = (int)ceil(numRows/numChunks);

	float corr;

	for(c = 0; c < numChunks; c++){
		for(i = 0; i < numCols - 1; i++){
			for(j = i+1; j < numCols; j++){

				sprintf(statcmd, "SELECT CORR(x, y) FROM (SELECT cast(%s as double precision) AS x, cast(%s as double precision) AS y FROM %s LIMIT %d OFFSET %d) as foo", 
					colList[i], colList[j], table, sizeChunk, c*sizeChunk);

				if(( result = PQexec( conn, statcmd )) == NULL && strlen(PQerrorMessage(conn)) > 2)   {
					printf( "%s\n", PQerrorMessage( conn ));
				}

				corr = atof(PQgetvalue(result, 0, 0));

				//printf("%f\n", corr);

				PQclear(result);

				sprintf(inscmd, "INSERT INTO dc_%s (col0, col1) VALUES (%d, %f)", table, idChunkCombine((long long)(pow(2,i) + pow(2,j)), c, numChunks), corr);

				if((result = PQexec( conn, inscmd )) == NULL){
					printf("well that was null\n" );
				}

				if (PQresultStatus(result) != PGRES_TUPLES_OK && strlen(PQerrorMessage(conn)) > 2)
			    {
			        printf("INSERT failed: %s\n", PQerrorMessage(conn));
			        PQclear(result);
			        //exit(0);
			    } else {
			    	PQclear(result);
			    }
			}
		}
	}
	PQfinish(conn);
}

int createDCTableLeveln(char* table, int levels, int numChunks, int numCols, int numRows){

	PGconn * conn;
	conn = PQconnectdb( "dbname=postgres" );
	PGresult * result;
	
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

							if(( result = PQexec( conn, statcmd )) == NULL && strlen(PQerrorMessage(conn)) > 2)   {
								printf( "SELECT ERROR: %s\n", PQerrorMessage( conn ));
							}
							counter++;

							vals[counter] = atof(PQgetvalue(result, 0, 0));
							PQclear(result);

						}
					}
			}
			correlation = 42.0;

			counter = 0;

			sprintf(inscmd, "INSERT INTO dc_%s (col0, col1) VALUES (%d, %f)", table, idChunkCombine(i, c, numChunks), correlation);

			if((result = PQexec( conn, inscmd )) == NULL){
				printf("well that was null\n" );
			}

			if (PQresultStatus(result) != PGRES_TUPLES_OK && strlen(PQerrorMessage(conn)) > 2)
		    {
		        printf("INSERT failed: %s\n", PQerrorMessage(conn));
		        PQclear(result);
		        //exit(0);
		    } else {
		    	PQclear(result);
		    }
		}
	}
	PQfinish(conn);
}

int main( int argc, char* argv[]){

	//rewrite to work with experiments
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

	if(strcmp(command, "setup") == 1 || strcmp(command, "all") == 1)
		createDCTableSetup(tableName, numCols, numChunks, numCols, numRows);
	if(strcmp(command, "level1") == 1 || strcmp(command, "all") == 1)
		createDCTableLevel1(tableName, numCols, numChunks, numCols, numRows);
	if(strcmp(command, "level2") == 1 || strcmp(command, "all") == 1)
		createDCTableLevel2(tableName, numCols, numChunks, numCols, numRows);
	if(strcmp(command, "leveln") == 1 || strcmp(command, "all") == 1)
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