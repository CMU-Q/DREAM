#include "Proxy.h"
extern int debug_level;

void doProxy() 
{	

	//printf("LDBL_MAX = %Lf\n", LDBL_MAX);

	initQueryPlanner();
	 
	int subQ_len, result_len, num_workers_to_send_results = 0;
	char *final_result = NULL, *partial_result = NULL;
	char *query = NULL, *subQ = NULL; ;
	char filename[20];
	MPI_Status status;
	FILE *stream;

	double start_t = 0, end_t = 0;
	int worker_traffic = 0;    // must change to long
	int total_traffic = 0;     // must change to long		

	char *result_set = NULL;
	short num_keys;
	char **keys;

	Node *node = NULL;		
	Query *Q = NULL;

//	time_t startTime, endTime;
	struct timeval start_tv;
	struct timeval end_tv;
	struct timeval diff_tv;
	
	struct timeval start_pq;
	struct timeval end_pq;
	struct timeval diff_pq;

	
	/***** receive query from the Client *****/
	MPI_URecv(&query, MPI_CHAR, CLIENT, Q_TAG, MPI_COMM_WORLD, &status);		
	debug_print(debug_level, COMM_LOG, "PROXY received from Client:\n%s\n", query);

	/***** create & assign sub-queries using the query planner *****/

	//time_t start_time = time(NULL);
	gettimeofday(&start_pq, NULL);
	Q = planQuery(query);
	gettimeofday(&end_pq, NULL);
	//time_t end_time = time(NULL);
	
	long int diffpq = (end_pq.tv_usec + 1000000 * end_pq.tv_sec) - (start_pq.tv_usec + 1000000 * start_pq.tv_sec);
	diff_pq.tv_sec = diffpq / 1000000;
	diff_pq.tv_usec = diffpq % 1000000;

	debug_print(debug_level, Q_PLAN_LOG,
	"Proxy finished the query planning in %ld.%06ld seconds\n", diff_pq.tv_sec, diff_pq.tv_usec);

	
	/***** send sub-queries to the assigned workers ****/
	debug_print(debug_level, Q_PLAN_LOG, 
		"---> Final number of join nodes: %hd\n", Q->num_join_nodes);

	debug_print(debug_level, Q_PLAN_LOG, 
		"---> Final number of merged nodes: %hd\n", Q->num_merged_nodes);
	
	//	startTime = time(NULL);		//aisha
	gettimeofday(&start_tv, NULL);

	
	/***** If one machine (the PROXY) can handle the client (<=1 join node) ****/
	if(Q->num_join_nodes <= 1
			|| Q->num_join_nodes - Q->num_merged_nodes <= 1)
	{

		// debug_print(debug_level, Q_PLAN_LOG, "%s\n", "Number of join nodes <= 1, or 0 workers");		
		// debug_print(debug_level, Q_PLAN_LOG, " PROXY executing the query %s\n", query);	
		
		// SubQInfo arg;
		// pthread_t subQ_tid;
		
		// arg.subQ   = query;
		// arg.w_rank = PROXY;

		//pthread_create(&subQ_tid, NULL, executeSubQuery, &arg);
				
		/** execute just on rdf, since there is no need to communicate data **/
		char infile[20];
		char outfile[20];
		char timefile[30];
		char command[100];
		FILE *streamP;

		sprintf(infile,  "subquery_%d.txt", PROXY);
		//sprintf(outfile, "result_%d.txt", PROXY);
		//sprintf(outfile, "Result-Q12", PROXY);
		//sprintf(timefile, "internal_time_%d.txt", PROXY);
	
		//printf("infile: \"%s\"\n", infile);
		
		if(!(streamP = fopen(infile, "w+"))) {
			systemError("Could not open subQ file");
			debug_print(debug_level, ERROR_LOG, "%s\n", "Could not open subQ file");
		}
		
		fprintf(streamP, "%s", query);
		fclose(streamP);

		sprintf(command, "./rdf3x/rdf3xquery %s < %s > %s", DB, infile, RESULT_FILE);
		
		int sysid = system(command);

		if(sysid < 0)
			debug_print(debug_level, ERROR_LOG, "%s\n", "Failed to issue make clean");					

	}
	/***** More than one machine is needed to handle query (>1 join node) ****/
	else 
	{
		// isWorkerNeeded = 1;		
		// MPI_Bcast(&isWorkerNeeded, 1, MPI_INT, 0, MPI_COMM_WORLD);
	
		// debug_print(debug_level, Q_PLAN_LOG, 
			// "Num OF join nodes : %hd\n", Q->num_join_nodes);	
		

		for(int i = 0; i < Q->num_join_nodes; i++)
		{
			node = Q->compactGraph[i];
			if(!node->is_deleted 
					&& node->subQ->optimalset != NULL)
			{	
				sendSubQToWorker(Q, node, &num_workers_to_send_results);	
			}
		}

		// debug_print(debug_level, COMM_LOG, "PROXY awaiting %d Worker(s) to reply\n", 
			// num_workers_to_send_results);		
			
				
		/***** receive partial results to union ****/
		for(int j = 0; j < num_workers_to_send_results; j++)	//what if there is more than one worker sending results?
		{	  
			int len = MPI_URecv(&partial_result, MPI_CHAR, MPI_ANY_SOURCE, 
				RESULT_SET_TO_UNION_TAG, MPI_COMM_WORLD, &status);			

			debug_print(debug_level, COMM_LOG, 
				"PROXY recvd final result of length %d from Worker %d\n",
				len, status.MPI_SOURCE);
				
			sprintf(filename, "result_%d.txt", PROXY);

			if((stream = fopen(filename,"w+")) == NULL) {
				systemError("Couldn't create a temp. result file");
				debug_print(debug_level, ERROR_LOG, "%s\n",  
					"Couldn't create a temp. result file");	
			}

				
			fwrite(partial_result, sizeof(char), len, stream);
			free(partial_result);
			fclose(stream);
		}
			
			/** issue a "make clean" **/ 
			char command_clean[256];
			sprintf(command_clean, "make clean\n");

			int syscleanid = system(command_clean);

			if(syscleanid < 0)
				debug_print(debug_level, ERROR_LOG, "%s\n", "Failed to issue make clean");
			
			/** sort the ids before converting - execvp version**/
			//int execvp(const char *file, char *const argv[]);
	
			/*char *argsvp[5];
			argsvp[0] = ;
			argsvp[1] = "-n";
			argsvp[2] = "-o";
			argsvp[3] = filename;
			argsvp[4] = filename;
			
			char commandsort[20];
			sprintf(commandsort, "sort -nr -o %s %s", filename, filename);
			printf("command to sort: \"%s\"\n", commandsort);
			char *argsvp[] = { "/bin/bash", "-c", commandsort, NULL};
			
			pid_t pid = fork();

			if (pid == -1) {
				perror("fork failed");
				exit(EXIT_FAILURE);
			}
			else if (pid == 0) {//child
				printf("before execvp\n");
				execvp(argsvp[0], argsvp);
			}
			else{
				int status;
				(void)waitpid(pid, &status, 0);
			}
			*/
			
			/** sort the ids before converting **/
			char command_sort[256];
			sprintf(command_sort, "sort -n -o %s %s", filename, filename);//aisha - change this line back
			
			//printf("filename \"%s\"\n", filename);
			//sprintf(command_sort, "sort -nr -o %s %s\0", filename, filename);	
			
			int syssortid = system(command_sort);

			if(syssortid < 0)
				debug_print(debug_level, ERROR_LOG, "%s\n", "Failed to sort IDs");
			
	
			/** remove previous result file **/
			char command_delete[256];
			sprintf(command_delete, "rm %s\n", RESULT_FILE);

			int sysdeltid = system(command_delete);

			if(sysdeltid < 0)
				debug_print(debug_level, ERROR_LOG, "%s\n", "Failed to send delete command to terminal");
			
			/** convert ids 2 names **/
			char zfilename[20];
			char ztfilename[20];
			char zcommand[100];
			sprintf(zfilename, "result_%d.txt\0", PROXY);
			sprintf(zcommand, "./id2name %s %s %s\n", 
				DB, zfilename, RESULT_FILE);
			
			//printf("DB: %s, zfilename: %s, RESULT_FILE: %s\n", DB, zfilename, RESULT_FILE);
			int sysid;			
			sysid = system(zcommand);

			if(sysid < 0)
				debug_print(debug_level, ERROR_LOG, "%s\n", "Failed to convert IDs to Names");
	
		
		
	 }	
	

	//endTime = time(NULL);		//aisha
	gettimeofday(&end_tv, NULL);
	
	/***** send result back to the Client ****/
	final_result = readFile(RESULT_FILE);

	if(
	(strcmp(final_result, "") == 0) ||
	(strcmp(final_result, "\n") == 0) ||
	(strcmp(final_result, "\0") == 0)
	){

		if((stream = fopen(RESULT_FILE,"w+")) == NULL) {
			systemError("Couldn't create a result file");
			debug_print(debug_level, ERROR_LOG, "%s\n",  
						"Couldn't create a result file");	
		}

		char *no_match = "  ********  No Match Found!  ********  \0";
	 
		fwrite(no_match, sizeof(char), strlen(no_match), stream);
		fclose(stream);

		final_result = readFile(RESULT_FILE);
	}
	
	MPI_Send(final_result, strlen(final_result),
		MPI_CHAR, CLIENT, Q_RESULT_TAG, MPI_COMM_WORLD);
	
//	debug_print(debug_level, COMM_LOG, "%s\n", "Proxy sent final result to Client"); aisha

	free(final_result);

	
//	int diff = endTime - startTime;
	long int diff = (end_tv.tv_usec + 1000000 * end_tv.tv_sec) - (start_tv.tv_usec + 1000000 * start_tv.tv_sec);
	diff_tv.tv_sec = diff / 1000000;
	diff_tv.tv_usec = diff % 1000000;

	//printf("dream: ");
	//printf("%ld.%06ld\n", diff_tv.tv_sec, diff_tv.tv_usec);

	//debug_print(debug_level, COMM_LOG, "TOTAL_TIME: %d\n", diff);	

}
