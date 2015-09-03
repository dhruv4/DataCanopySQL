/* Processed by ecpg (4.8.0) */
/* These include files are added by the preprocessor */
#include <ecpglib.h>
#include <ecpgerrno.h>
#include <sqlca.h>
/* End of automatic include section */

#line 1 "pgCtest.pgc"
#include <stdio.h>                                                                                                           
#include <stdlib.h>
#include <string.h>
#include <math.h>

typedef enum { false, true } bool;
void tobinstr(int value, int bitsCount, char* output)
{
    int i;
    output[bitsCount] = '\0';
    for (i = bitsCount - 1; i >= 0; --i, value >>= 1)
    {
        output[i] = (value & 1) + '0';
    }
}
int count1s( char* s )
{
	int i;
	for (i=0; s[i]; s[i]=='1' ? i++ : *s++);
    return i;
}

bool checkLevel1(int x){
	while(((x % 2) == 0) && x > 1){
		x >>= 1;
	}
	return (x == 1);
}

bool checkLevel2(int x){

	char output[32];

	tobinstr(x, 32, output);

	if(count1s(output) == 2){
		return 1;
	}
	return 0;
		
}

void createTable(char* name, int numCol, bool b, bool l){
	
	char cols[] = "(";
	int x;

	if(b){
		if(l){
			strcat(cols,"col0 bigint PRIMARY KEY,");
		} else {
			strcat(cols,"col0 int PRIMARY KEY,");
		}
		for(x = 0; x < numCol; x++){
			strcat(cols, "col");
			strcat(cols, x + " double precision,");
		}
	} else {
		for(x = 0; x < numCol; x++){
			strcat(cols, "col");
			strcat(cols, x + " int,");
		}
	}

	cols[strlen(cols)-1] = 0;

	strcat(cols, ")");

	//cur.execute("CREATE TABLE " + name + " " + cols)

}

int idChunkCombine(int idn, int chunk, int numChunks){
	return ((idn << (int)ceil(log(numChunks)/log(2))) | chunk);
}

void createDCTableSetup(char* table, int levels, int numChunks, int numCols, int numRows){
	
	//conn = pg.connect(dbname="postgres")
	//cur = conn.cursor()

	if(numCols + (int)ceil(log(numChunks)/log(2)) >= 32){
		//createTable('dc_' + table, 6, 1, 1);
	} else {
		//createTable('dc_' + table, 6, 1);
	}

	//conn.commit()
}
int createDCTableLevel1(char* table, int levels, int numChunks, int numCols, int numRows){

	//conn = pg.connect(dbname="postgres")
	//cur = conn.cursor()

	//cur.execute("SELECT column_name from information_schema.columns where table_name='" + table + "'")
	
	//colList = [x[0] for x in cur.fetchall()]

	int maxRows = (pow(2,numCols) - 1)*numChunks;
	int sizeChunk = (int)ceil(numRows/numChunks);

	int ID = 1, c, i;
	for(c = 0; c < numChunks; c++){
		for(i = 0; i < numCols; i++){
			
			/*cur.execute("SELECT AVG(ss), STDDEV(ss), VAR_SAMP(ss) FROM (SELECT " 
				+ colList[i] + " AS ss FROM " 
				+ table + " LIMIT " + str(sizeChunk) 
				+ " OFFSET " + str(c*sizeChunk) + ") as foo")
			*/

			//int avg, int std, int var = cur.fetchone()
			
			int avg = 0, std = 0, var = 0;

			int med = 0; //median

			//cur.execute("SELECT TOP 1 COUNT( ) val, freq FROM " + table + " GROUP BY " + colList[j] + " ORDER BY COUNT( ) DESC")
			//mod = int(cur.fetchone()[0])
			int mod = 0;

			ID = 1<<i;

			ID = idChunkCombine(ID, c, numChunks);

			//cur.execute("INSERT INTO dc_" + table + " (col0, col1, col2, col3, col4, col5) VALUES (%s, %s, %s, %s, %s, %s)",
			//	[ID, avg, std,var,med,mod])
		}
	}
	//conn.commit()
}

int createDCTableLevel2(char* table, int levels, int numChunks, int numCols, int numRows){
	
	//conn = pg.connect(dbname="postgres")
	//cur = conn.cursor()

	//cur.execute("SELECT column_name from information_schema.columns where table_name='" + table + "'")
	//colList = [x[0] for x in cur.fetchall()]

	int maxRows = (pow(2,numCols) - 1)*numChunks;
	int sizeChunk = (int)ceil(numRows/numChunks);

	int c, i, j;

	for(c = 0; c < numChunks; c++){
		for(i = 0; i < numCols - 1; i++){
			for(j = i+1; j < numCols; j++){
				//cur.execute("SELECT CORR(x, y) FROM (SELECT cast(" + colList[i] + " as double precision) AS x, cast(" 
				//	+ colList[j] + " as double precision) AS y FROM " 
				//	+ table + " LIMIT " + str(sizeChunk) 
				//	+ " OFFSET " + str(c*sizeChunk) + ") as foo")

				//####^^^^ This HAS to be the slowest statement right?

				//cur.execute("INSERT INTO dc_" + table + " (col0, col1) VALUES (%s, %s)", 
				//	[idChunkCombine(2**i + 2**j, c, numChunks),float(cur.fetchone()[0])])
			}
		}
	}
	//conn.commit()
}

int createDCTableLeveln(char* table, int levels, int numChunks, int numCols, int numRows){

	//conn = pg.connect(dbname="postgres")
	//cur = conn.cursor()

	int c, i, x, y, correlation;

	for(c = 0; c < numChunks; c++){
		for(i = 1; i < pow(2, numCols); i++){
			if(checkLevel1(i) == 1 || checkLevel2(i) == 1)
				continue;
			
			int vals[numCols*numCols];
			for(x =0; x < numCols; x++){
				if((i >> x) & 1 == 1)
					for(y = x+1; y < numCols; y++){
						if((i >> y) & 1 == 1){
							//cur.execute("SELECT col1 FROM dc_" + table + " WHERE col0 = " 
							//	+ str(idChunkCombine(2**x + 2**y, c, numChunks)))
							
							//vals.append(cur.fetchone()[0])	
						}
					}
			}
			correlation = 42;

			//cur.execute("INSERT INTO dc_" + table + " (col0, col1) VALUES (%s, %s)", 
			//	[idChunkCombine(i, c, numChunks), correlation])
		}
	}
	//conn.commit()
}

int main( int argc, char* argv[]){

	int x;
	for(x = 0; x < argc; x++){
		printf("%s\n", argv[x]);
	}

	{ ECPGconnect(__LINE__, 0, "postgres" , "postgres" , NULL , "con1", 1); }
#line 204 "pgCtest.pgc"


	printf("%d\n", checkLevel2(6));
	printf("%d\n", checkLevel2(4));

	return 0;
}