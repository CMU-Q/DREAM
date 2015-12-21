#include "Worker.h"

/**
 * This is the Worker function that performs all the slave's functionalities.
 * It does the following:
 * 	1) It first receives the keys and subquery assigned for that specific slave.
 * 	2) It then processes this subquery.
 * 	3) For every in-degree for that join-node, it receives a communicator 
 *     request and proceeds to create one 
 *  4) It recieves neighbor results using that communicator and then hash-joins
 *  5) For every out-degree for that join-node, it creates a communicator
 *     and sends its out-degree neighbors create-communicator-requests
 *	6) After this node receives all the neighbor results and concludes all
 *	   hash joins, it sends the final result back to the proxy.
 */
 
void doWorker() 
{
	
	int my_rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

	extern int debug_level;
	char *my_subQ_msg, *my_subQ, **my_keys, *my_partial_result;
	short my_in_degree, my_out_degree, my_num_keys, final_num_keys;
	char *final_keys_msg, **final_keys;
	int *my_ngbrs_ranks;
	MPI_Status status, status1;;
	int msg_len;		

	pthread_t subQ_tid;
	pthread_t ngbr_tid;
	short ngbr_num_keys;
	char **ngbr_keys;

	char *my_metadata;
	char *my_result_set;
	char *ngbr_metadata;
	char *ngbr_result_set = NULL;

	int comm_size;
	int *comm_ranks;
	MPI_Comm NGBRS_COMM;
	
	int my_new_rank;	//aisha

	//loadRDF();

	/***** receive final keys message from the Proxy *****/
	MPI_URecv(&final_keys_msg, MPI_BYTE, PROXY, FINAL_KEYS_TAG, MPI_COMM_WORLD, &status);
	//debug_print(debug_level, COMM_LOG, "%s\n", "receive final keys message from the Proxy"); aisha

	/***** parse final keys message *****/
	parseFinalKeysMessage(final_keys_msg, &final_num_keys, &final_keys);
	
	/***** receive sub-query message from the Proxy *****/
	//debug_print(debug_level, COMM_LOG, "%s\n", "receive sub-query message from the Proxy"); aisha

	MPI_URecv(&my_subQ_msg, MPI_BYTE, PROXY, SUBQ_TAG, MPI_COMM_WORLD, &status);
	
	//debug_print(debug_level, COMM_LOG, "Worker %d received sub-query from Proxy\n", my_rank); aisha
	

	/***** parse sub-query message *****/
	parseSubQMessage(my_subQ_msg, &my_in_degree, &my_out_degree, &my_ngbrs_ranks,
		&my_num_keys, &my_keys, &my_subQ);

	
	/***** parse sub-query *****/
	SubQInfo tid1_arg;
	tid1_arg.subQ   = my_subQ;
	tid1_arg.w_rank = my_rank;

	ResultInfo tid2_arg;
	//tid2_arg.ngbr_result_set = &ngbr_result_set;
	tid2_arg.my_rank = my_rank;
	
	//executeSubQuery(my_subQ, my_rank);
	pthread_create(&subQ_tid, NULL, executeSubQuery, &tid1_arg);
	

	/***** receive all partial results from in-neighbours and hash join *****/			
	pthread_t prTids[my_in_degree];
	
	MPI_Comm NGBRS_COMM_LIST[my_in_degree];
	
	void *exit_status;

	if(pthread_join(subQ_tid, exit_status) != 0)	//execute own subQ first
	{
		printf("Error joining thread\n");
		// fflush(stdout);
	} else {					//convert results
			
		char filename[20];			//why are we converting results?	//aisha
		char tfilename[20];
		char command[100];
		sprintf(filename, "result_%d.txt", my_rank);
		sprintf(tfilename, "result_%d_tmp.txt", my_rank);
		sprintf(command, "./id2name %s %s %s", DB, filename, tfilename);

		//if(system(command) < 0)
		//	debug_print(debug_level, ERROR_LOG, "%s\n", "Failed to convert IDs to Names");	

//		printf("after printing command\n");
				
	}

	//printf("\t\t########## \n\t\tWORKER %d in_degree = %d \n\t\t##########\n", my_rank, my_in_degree);	//aisha		

	debug_print(debug_level, COMM_LOG, "Worker %d about to recv results from ngbrs\n", my_rank);
		
	//recv results from in-neighbours
	for(int num_results_recvd = 0; num_results_recvd < my_in_degree; num_results_recvd++)
	{			
		//printf("\n\n\t\t########## \n\n\t\tWORKER %d in_degree = %d \n\n\t\t##########\n", my_rank, my_in_degree);	//aisha		

		// rcvPartialResults.num_results_recvd = num_results_recvd;
		// pthread_create(&prTids[num_results_recvd], NULL, recievePartialResultsT2, &rcvPartialResults);
		// recievePartialResultsT2(&rcvPartialResults);
		
		// pthread_join(prTids[num_results_recvd], &exit_status);
		// recievePartialResults(&comm_size, &comm_ranks, &NGBRS_COMM, &tid2_arg, &ngbr_metadata, 
			// &ngbr_num_keys, &ngbr_keys, &my_result_set, &ngbr_tid, &msg_len, num_results_recvd, 
			// my_in_degree, &my_num_keys, &my_keys, &ngbr_result_set, &final_num_keys, &final_keys, &subQ_tid);
		 
		// insert in one thread/function

		recvCreateCommunicatorRequest(&comm_size, &comm_ranks);

		createCommunicator(&NGBRS_COMM, comm_size, comm_ranks);
				
		//sendCreateCommunicatorAck(&comm_size, &comm_ranks);
		MPI_URecv(&ngbr_metadata, MPI_CHAR, MPI_ANY_SOURCE, METADATA_TAG, MPI_COMM_WORLD, &status);

		MPI_Comm_rank(NGBRS_COMM, &my_new_rank);	//aisha

		parseMetadataMessage(ngbr_metadata, &ngbr_num_keys, &ngbr_keys);
		//printf("WORKER %d (new rank %d) successfully parsed metadata\n", my_rank, my_new_rank); //aisha
//		printf("\t num_ngbr_keys = %d (", ngbr_num_keys);

//		for(int k = 0; k < ngbr_num_keys; k++){	
//			printf("%s, ", ngbr_keys[k]);		
//		}
//		printf(")\n");

		debug_print(debug_level, COMM_LOG, "Worker %d recvd metadata from Worker %d\n", my_rank, status.MPI_SOURCE);	//aisha
		free(ngbr_metadata);		

		void *exit_status;
		tid2_arg.NGBRS_COMM = &NGBRS_COMM;
		tid2_arg.ngbr_rank = status.MPI_SOURCE;
		tid2_arg.ngbr_result_set = &my_result_set; 
		tid2_arg.msg_len = &msg_len; 
		
		//printf("WORKER %d num_results_rcvd before rcvNGBR results: {%d}\n", my_rank, num_results_recvd);
		
		pthread_create(&(prTids[num_results_recvd]), NULL, receiveNgbrResult, &tid2_arg);
		
		//printf("WORKER %d num_results_rcvd after rcvNGBR results: {%d}\n", my_rank, num_results_recvd);
		

		if(num_results_recvd == (my_in_degree - 1))
		{
			debug_print(debug_level, COMM_LOG,
			"Worker %d recving final result from Worker %d\n",
			my_rank, status.MPI_SOURCE);	//aisha
		
			joinHelper(my_rank, &my_num_keys, &my_keys, ngbr_num_keys, ngbr_keys,  
				ngbr_result_set, final_num_keys, final_keys, subQ_tid, prTids[num_results_recvd]);
		}

		else
		{
			debug_print(debug_level, COMM_LOG,
			"Worker %d recving result from Worker %d\n",
			my_rank, status.MPI_SOURCE);	//aisha
		
			joinHelper(my_rank, &my_num_keys, &my_keys, ngbr_num_keys, ngbr_keys, 
				ngbr_result_set, 0, NULL, subQ_tid, prTids[num_results_recvd]);
		}				
		
						
	}

	//end of recv ngbr results

		// printf("\nMY_RANK %d NOW\n", my_rank);
		// printf("MY_RANK %d NOW\n", my_rank);
		// printf("MY_RANK %d NOW\n", my_rank);
		// MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
		// printf("MY_RANK %d NOW\n", my_rank);
		// printf("MY_RANK %d NOW\n\n", my_rank);

	/***** broadcast result message (to be joined) to all out-neighbors or proxy (if none) *****/
	
	//int new_rank;
	//MPI_Barrier(NGBRS_COMM);
	//MPI_Comm_rank(NGBRS_COMM, &new_rank);
	//printf("Worker's %d new rank is %d\n", my_rank, new_rank);

// Send a create communicator message to all out-neighbors
	createCommunicatorRanksArray(my_rank, my_ngbrs_ranks, my_out_degree, &comm_size, &comm_ranks);	
			
	sendCreateCommunicatorRequest(my_out_degree, my_ngbrs_ranks, comm_size, comm_ranks);

//aisha OLD CREATE COMM FOR SENDER WAS HERE		
//aisha START COMM create
/*		printf("\t creating communicator for: ");
		for(int k=0; k<comm_size; k++){
			printf("%d, ", comm_ranks[k]);		
		}
		printf("\n");
*/


	createCommunicator(&NGBRS_COMM, comm_size, comm_ranks);
//aisha		debug_print(debug_level, COMM_LOG, "Worker %d created a new communicator\n", my_rank);

//aisha END COMM create

	
	msg_len = createMetadataMessage(&my_metadata, my_keys, my_num_keys);

//aisha
	MPI_Comm_rank(NGBRS_COMM, &my_new_rank);	//aisha

	sendMyMetadata(my_metadata, msg_len, my_rank, my_out_degree, my_ngbrs_ranks);

	free(my_metadata);
	

//aisha	// Wait for the subQ thread to terminate before sending the result set - but aren't we already doing this at the top?
	if(my_in_degree == 0)
	{
		
		void *exit_status;
		if(pthread_join(subQ_tid, &exit_status) != 0)
		{
			// printf("Error joining thread\n");
		//debug_print(debug_level, ERROR_LOG, "%s\n", "Error joining thread"); aisha
			
			fflush(stdout);
		} 
		else
		{
			// printf("SUBQ THREAD SUCCESSFULLY JOINED\n");
			//debug_print(debug_level, ERROR_LOG, "%s\n", "SUBQ THREAD SUCCESSFULLY JOINED");
			fflush(stdout);
		}
	} 
	else
	{

	}



	msg_len = createResultMessage(&my_result_set, my_rank);
	//debug_print(debug_level, COMM_LOG, "Worker %d created a message of length %d\n", my_rank, msg_len);	aisha

	if(my_out_degree == 0)
	{	
		sendMyResult(my_result_set, msg_len, my_rank, my_out_degree, my_ngbrs_ranks);
	} 
	else
	{

//aisha OLD CREATE COMM FOR SENDER WAS HERE		

//aisha		recvCreateCommunicatorRequest(&comm_size, &comm_ranks);	//why does this need to be here?
		
//aisha OLD SENDING & FREEING METADATA WAS HERE

		MPI_Bcast(&msg_len, 1, MPI_INT, 0, NGBRS_COMM);

		//debug_print(debug_level, COMM_LOG, "%s\n", "MPI_Bcast IN WORKER: MPI_Bcast(&msg_len, 1, MPI_INT, 0, NGBRS_COMM)");
		debug_print(debug_level, COMM_LOG, "Worker %d sent that the length of the message is %d\n", my_rank, msg_len);
		
		for(int k = 0; k < my_out_degree; k++){
			debug_print(debug_level, COMM_LOG, "Worker %d neighbor rank %d\n", my_rank, my_ngbrs_ranks[k]);
		}
		
		MPI_Bcast(my_result_set, msg_len, MPI_CHAR, 0, NGBRS_COMM);	
		
		debug_print(debug_level, COMM_LOG, "Worker %d sent the message\n", my_rank);
	}

	//printf("WORKER %d finished\n", my_rank);				
}
