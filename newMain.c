#include "Main.h"

int my_rank, query_ndx;
int SHOULD_GET_NETWORK_SPEED;
int DESIRED_PLAN_NDX = 1;
int debug_level = 0;							 

char TIME_FILE[100];
char VOLUME_FILE[100];

char QUERY_FILE[100];
char RESULT_FILE[100];

char STAT_FILE_1[100];
char STAT_FILE_2[100];

char STAT_FILES[100];
char QUERIES[100];
char DB[100];


/*
	Function to print in usage instructions
*/
void print_usage() {
    printf("Usage: ./exec.sh [File name that contains the following:] \n");
    printf("	[Query Index] (Greater than 0) \n");
    printf("	[SHOULD_GET_NETWORK_SPEED] \n");
    printf("	[TIME_FILE] \n");
    printf("	[VOLUME_FILE] \n");
    printf("	[QUERIES] \n");
    printf("	[QUERY] \n");
    printf("	[STAT_FILES] \n");
    printf("	[STAT_FILE_1] \n");
    printf("	[STAT_FILE_2] \n");
    printf("	[RESULT_FILE] \n");
    printf("	[DATABASE] \n");
    printf("	[DEBUG_LEVEL] 1: MISC. ERRORS\n");
    printf("	              2: MPI COMMUNICATION\n");
    printf("	              3: QUERY PLANNING INFO\n");
}

/*
	Recieve File from user, parse and assign variables accordingly from 
	file and begin execution
*/
int main(int argc,char *argv[])
{

	// Initialization
	int num_procs;
	
	if(argc != 2)
	{
		print_usage();
		return 0;
	}
	
	char file_name[100];
	
	sprintf(file_name, "%s", argv[1]);
	
	FILE *fp;
	fp = fopen(file_name,"r"); // read mode
 
   if(fp==NULL)
	{
		perror("Error while opening the file.\n");
		print_usage();
		exit(EXIT_FAILURE);
	}
   
	char buf[256];
	
	memset(TIME_FILE, 0, 100);
	memset(VOLUME_FILE, 0, 100);
	memset(STAT_FILE_1, 0, 100);
	memset(STAT_FILE_2, 0, 100);
	memset(QUERY_FILE,  0, 100);
	memset(RESULT_FILE, 0, 100);

	// Retrieving results from input
	int currentInput=0;
	size_t len;
	while (fgets (buf, sizeof(buf), fp)) 
	{
		len = strlen(buf);
		if (len > 0 && buf[len-1] == '\n') buf[len-2] = '\0';

        switch (currentInput) {   		
			// [Query Index] (Greater than 0)
            case 0 : 
				query_ndx=atoi(buf);
				if(query_ndx < 0){ 
					printf("Query index must be integer");
					print_usage();
					return 0;
				}
                break;
			// [SHOULD_GET_NETWORK_SPEED]
			case 1:
				SHOULD_GET_NETWORK_SPEED=atoi(buf);
				break;
             case 2 : 
			// [TIME_FILE]
				sprintf(TIME_FILE, "Time-Q%d", query_ndx);
                break;			 
			// [VOLUME_FILE]
			 case 3 : 
				sprintf(VOLUME_FILE, "Volume-Q%d", query_ndx);
                break;
			// [QUERIES]
  			 case 4 : 
				sprintf(QUERIES, "%s", buf);
                break;
			// [QUERY]
			 case 5 : 
				sprintf(QUERY_FILE,  "%s/Query%d", QUERIES, query_ndx);
                break;
			// [STAT_FILES]
             case 6 : 
				sprintf(STAT_FILES, "%s", buf);
                break;
			// [STAT_FILE_1]
			 case 7 : 
				sprintf(STAT_FILE_1, "%s/LUBMResultSizeStats-%d.txt", STAT_FILES, query_ndx);
				break;	   
			// [STAT_FILE_2]
			 case 8 : 
				sprintf(STAT_FILE_2, "%s/LUBMCostStats-%d.txt", STAT_FILES, query_ndx);
                break;
			// [RESULT_FILE]
			 case 9 : 
				sprintf(RESULT_FILE, "Result-Q%d", query_ndx);
                break;
			// [DATABASE]
			 case 10 : 
				sprintf(DB, "%s", buf); 
                break;
			// [DEBUG_LEVEL]	
			 case 11 : 
				debug_level=atoi(buf); 
                break;	
			 default:
				printf("Invalid argument number given\n");
				print_usage();			
				return 0;
		}   
	   currentInput+=1;
	}

	// printf("------------------------------------------\n");
		
	sprintf(TIME_FILE, "Time-Q%d", query_ndx);
	sprintf(VOLUME_FILE, "Volume-Q%d", query_ndx);
	sprintf(QUERY_FILE,  "%s/Query%d", QUERIES, query_ndx);
	sprintf(RESULT_FILE, "Result-Q%d", query_ndx);
	
	sprintf(STAT_FILE_1, "LUBMResStats-%d.txt", query_ndx);
	sprintf(STAT_FILE_2, "LUBMCostStats-%d.txt", query_ndx);


	// printf("------------------------------------------\n");
	// printf("Values Retrieved:\n");
	// printf("QNDX:%d \n",query_ndx);
	// printf("TIME_FILE:%s \n",TIME_FILE);
	// printf("VOLUME_FILE:%s \n",VOLUME_FILE);
	// printf("QUERY_FILE:%s \n",QUERY_FILE);
	// printf("RESULT_FILE:%s \n",RESULT_FILE);
	// printf("STAT_FILE_1:%s \n",STAT_FILE_1);
	// printf("STAT_FILE_2:%s \n",STAT_FILE_2);
	// printf("STAT_FILES:%s \n",STAT_FILES);
	// printf("QUERIES:%s \n",QUERIES);
	// printf("DB:%s \n",DB);		
	//SHOULD_GET_NETWORK_SPEED = atoi(argv[2]);

	// MPI_Init(&argc, &argv);
	
	int provided; 
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE , &provided);
	if(provided!=MPI_THREAD_MULTIPLE)
		debug_print(debug_level, ERROR_LOG, "s\n", "MPI_THREAD_MULTIPLE not supported!");
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
	MPI_Errhandler_set(MPI_COMM_WORLD, MPI_ERRORS_RETURN);

	// Handle CLIENT, PROXY and WORKER tasks accordingly
	if (my_rank == CLIENT)
	{
		doClient();
	}
	
	else if (my_rank == PROXY)
	{
		/********runtimes of queries [2, 7, 8, 9, 12] on rdf********/
		char command_rdf[256];
		char infile[256];
		char outfile[256];
		struct timeval start_tv;
		struct timeval end_tv;
		struct timeval diff_tv;

		sprintf(infile,  "%s/Query%d\0", QUERIES, query_ndx);	
		sprintf(outfile, "Result-Q%d-rdf\0", query_ndx);	
		sprintf(command_rdf, "./DATA-EXECUTABLE/rdf3xquery %s < %s > %s", 
						DB, infile, outfile);

		gettimeofday(&start_tv, NULL);
		system(command_rdf);
		gettimeofday(&end_tv, NULL);

		long int diff = (end_tv.tv_usec + 1000000 * end_tv.tv_sec) - (start_tv.tv_usec + 1000000 * start_tv.tv_sec);
		diff_tv.tv_sec = diff / 1000000;
		diff_tv.tv_usec = diff % 1000000;

		printf("rdf3x: ");
		printf("%ld.%06ld\n", diff_tv.tv_sec, diff_tv.tv_usec);
		/*********end runtimes of queries on rdf**********/
		
		doProxy();
	}
	
	// SLAVES
	else
	{
		doWorker();
	} 

	debug_print(debug_level, COMM_LOG, "MAIN: process with rank %d EXITED\n", my_rank);
	
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();
	return 0;
}

