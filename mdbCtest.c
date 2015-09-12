//mdbCtest.c

#include <stdio.h>                                                                                                           
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <mapi.h>

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
	if (mapi_close_handle(ret) != MOK)
		die(dbh, ret);
}

int main( int argc, char* argv[]){

	int numRows, numChunks, numCols;

	if(argc > 1){
		numRows = atoi(argv[1]);
		numChunks = atoi(argv[2]);
		numCols = atoi(argv[3]);
	} else {
		numRows = 10000;
		numChunks = 10;
		numCols = 10;
	}

	int x;
	for(x = 0; x < argc; x++){
		printf("%s\n", argv[x]);
	}

	Mapi dbh;
	MapiHdl hdl = NULL;

	dbh = mapi_connect("dhruv-VirtualBox", 50000,"monetdb", "monetdb", "sql", "test");

	hdl = query(dbh, "SELECT * FROM banana");

	 while (mapi_fetch_row(hdl)) {
	     printf("%d is %d\n", atoi(mapi_fetch_field(hdl, 0)), atoi(mapi_fetch_field(hdl, 1)));
	 }

	 printf("%f\n", log(4));

	return 0;
}