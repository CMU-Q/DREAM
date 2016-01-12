#include "Main.h"

int my_rank, query_ndx;
int SHOULD_GET_NETWORK_SPEED;
int SHOULD_LOAD_STATS;
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
    printf("Usage: mpiexec -f machinefile ./dream input.txt [File name that contains the following:] \n");
    printf("	[QUERY NUMBER] (Greater than 0) \n");
    printf("	[SHOULD_GET_NETWORK_SPEED] \n");
    printf("	[PATH TO FOLDER CONTAINING QUERIES] \n");
    printf("	[PATH TO FOLDER CONTAINING STAT_FILES] \n");
    printf("	[PATH TO DATABASE] \n");
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
		perror("Error while opening the input file.\n");
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
//	while (fgets (buf, sizeof(buf), fp)) - change for dynamic num args later
	for(int k = 0; k < 6; k++)
	{
		fgets(buf, sizeof(buf), fp);
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
		// [PATH_TO_QUERIES]
		case 2 :
			sprintf(QUERIES, "%s", buf);
			break;		 
		// [PATH_TO_STATS_FILES]
		case 3 :
			sprintf(STAT_FILES, "%s", buf); 
			break;
		// [PATH_TO_DATABASE]
		case 4 : 
       			sprintf(DB, "%s", buf); 
			break;
		// [DEBUG_LEVEL]
		case 5 : 
			debug_level=atoi(buf); 
			break;
		default:
			printf("Invalid argument number given\n");
			print_usage();			
			return 0;
		}   
	   currentInput+=1;
	}

	sprintf(QUERY_FILE,  "%s/Query%d", QUERIES, query_ndx);
	sprintf(STAT_FILE_1, "%s/LUBMResStats-%d.txt", STAT_FILES, query_ndx);
	sprintf(STAT_FILE_2, "%s/LUBMCostStats-%d.txt", STAT_FILES, query_ndx);
	sprintf(RESULT_FILE, "Result-Q%d", query_ndx);
	
	FILE *statsfile;

	if(!(statsfile = fopen(STAT_FILE_1, "r"))){
                SHOULD_LOAD_STATS = 0;
	}
	else{
		fclose(statsfile);

		if(!(statsfile = fopen(STAT_FILE_2, "r"))){
                	SHOULD_LOAD_STATS = 0;
		}
		else{
			fclose(statsfile);
			SHOULD_LOAD_STATS = 1;
		}
	}

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
		/********runtimes of queries [2, 7, 8, 9, 12] on rdf*********/
		char command_rdf[256];
		char infile[256];
		char outfile[256];
		struct timeval start_tv;
		struct timeval end_tv;
		struct timeval diff_tv;

		sprintf(infile,  "%s/Query%d\0", QUERIES, query_ndx);	
		sprintf(outfile, "Result-Q%d-rdf\0", query_ndx);	
		sprintf(command_rdf, "./rdf3x/rdf3xquery %s < %s > %s", 
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

