#include "WorkerUtil.h"

/**
 * This is a set of helper functions used by the WORKER.
 */

#define MAX_LENGTH 500

extern int debug_level;

/**
 * Parses the final_keys received from the PROXY
 */
void parseFinalKeysMessage(char *message, short *final_num_keys, char **final_keys[])
{
	int offset;
	char *p = message;
	short *a;
	
	*final_num_keys = parseShort(message); 
		
	p += 2;

	if(!(*final_keys = (char**) malloc(sizeof(char*) * (*final_num_keys)))) {
		systemError("Could not allocate ranks in parseFinalKeysMessage");
		debug_print(debug_level, ERROR_LOG, "%s", "Could not allocate ranks in parseFinalKeysMessage");
	}

	for(int i = 0; i < *final_num_keys; i++)
	{
		if(sscanf(p, " %m[?a-zA-Z0-9_]%n", &((*final_keys)[i]), &offset) != 1)
		break;
		
		p += offset;

	}
}

// parse the sub-query message received from the proxy..all parameters are populated with
// information about the current worker.
void parseSubQMessage(char *message, short *in_degree, short *out_degree, int **ranks,
short *num_keys, char **keys[], char **subQ)
{
	char *p = message;

	*in_degree = parseShort(p);   p += 2;
	*out_degree = parseShort(p);  p += 2;

	if(!(*ranks = (int *) malloc(sizeof(int) * (*out_degree))))
	systemError("Could not allocate ranks in parseSubQMessage");

	for(int i = 0; i < *out_degree; i++)
	{
		(*ranks)[i] = parseInt(p);
		p += 4;
	}	

	*num_keys = parseShort(p); p += 2;

	*subQ = p;

	if(!(*keys = (char **) malloc(sizeof(char *) * (*num_keys))))
	systemError("Could not allocate results in parseSubQMessage");
	
	p += strlen("select ");

	for(int i = 0; i < *num_keys; i++)
	{
		int offset;
		if(sscanf(p, " %m[?a-zA-Z0-9_]%n", &((*keys)[i]), &offset) != 1)
		break;
		p += offset;
	}
}


int createMetadataMessage(char **message, char *keys[], short num_keys)
{
	int message_len = 0;

	for(int i = 0; i < num_keys; i++){
		message_len += (sizeof(int8_t) * (strlen(keys[i]) + 1));
	}

	message_len += sizeof(int16_t);
	message_len += sizeof(int8_t);

	if(!(*message = (int8_t *) malloc(sizeof(int8_t) * (message_len)))) {
		systemError("Could not allocate message in createMetadataMessage");
		debug_print(debug_level, ERROR_LOG, "%s\n", "Could not allocate message in createMetadataMessage");
	}

	char *m = *message;

	num_keys = htons(num_keys);

	memcpy(m, (int8_t *)&num_keys, 2);
	m += 2;

	for(int i = 0; i < ntohs(num_keys); i++)
	{
		int len = strlen(keys[i]);
		memcpy(m, (int8_t *)keys[i], len);
		m += len;

		memcpy(m, (int8_t *)" ", 1);
		m += 1;
	}

	memcpy(m, (int8_t *)"\0", 1);

	return message_len;
}

int createResultMessage(char **message, int worker_id)
{
	int message_len = 0;
	char filename[20];
	char ch;
	
	sprintf(filename, "result_%d.txt", worker_id);
	FILE *stream = fopen(filename, "r");
	fseek(stream, 0L, SEEK_END);
	int fsize = ftell(stream);
	fseek(stream, 0L, SEEK_SET);
	
	message_len +=  (sizeof(int8_t) * fsize);
	//message_len +=  sizeof(int8_t); //aisha

	//if(!(*message = (int8_t *) malloc(sizeof(int8_t) * (message_len)))) { //- AISHA 
	if(!(*message = (int8_t *) malloc(message_len))) {
		systemError("Could not allocate message in createResultMessage");
		debug_print(debug_level, ERROR_LOG, "%s\n", "Could not allocate message in createResultMessage");
	}			

	char *m = *message;

	fread(m, sizeof(int8_t), fsize, stream);
	fclose(stream);
	m += fsize;

	//memcpy(m, (int8_t *)"\0", 1); aisha

	return message_len;
}


void parseMetadataMessage(char *message, short *num_keys, char **keys[])
{
	char *p = message;
	int offset;

	*num_keys = parseShort(p);   p += 2;
	
	if(!(*keys = (char **)(malloc(sizeof(char *) * (*num_keys))))) {
		systemError("Could not allocate results in parseMetadataMessage");
		debug_print(debug_level, ERROR_LOG, "%s\n", "Could not allocate results in parseMetadataMessage");
	}		

	for(int i = 0; i < *num_keys; i++)
	{
		if(sscanf(p, " %ms%n", &((*keys)[i]), &offset) != 1)
		break;

		p += offset;
	}
}

void parseResultMessage(char *message, char **result_set)
{
	*result_set = message;
}


// parse the result message received from the neighbor and populate the parameters with
// information about the neigbhor's query.
/*void parseResultMessage(char *message, short *num_results, char **results[], char **result_set)
{
	char *p = message;
	int offset;

	*num_results = parseShort(p);   p += 2;

	if(!(*results = (char **)(malloc(sizeof(char *) * (*num_results)))))
	systemError("Could not allocate results in parseResultMessage");
	
	for(int i = 0; i < *num_results; i++)
	{
		if(sscanf(p, " %ms%n", &((*results)[i]), &offset) != 1)
					break;
		
			p += offset;
	}	

	*result_set = p + 1;
}*/

/**
 * The following create the communicators to send neighbor results effectively
 */


void createCommunicatorRanksArray(int my_rank, int *my_ngbrs_ranks, int my_num_ngbrs, 
int *comm_size, int **comm_ranks)
{
	*comm_size  = my_num_ngbrs + 1;	//because need to include self
	*comm_ranks = (int *) malloc(sizeof(int) * (*comm_size));
	
	memcpy(*comm_ranks, &my_rank, sizeof(int));
	memcpy(*comm_ranks + 1, my_ngbrs_ranks, my_num_ngbrs * sizeof(int));
}

void sendCreateCommunicatorRequest(int num_ngbrs, int *ngbrs_ranks, int comm_size, int *comm_ranks)
{
	for(int i = 0; i < num_ngbrs; i++)
	{
		MPI_Send(&comm_size, 1, MPI_INT, ngbrs_ranks[i], NUM_NGBRS_TAG, MPI_COMM_WORLD);
		MPI_Send(comm_ranks, comm_size, MPI_INT, ngbrs_ranks[i], NGBRS_RANKS_TAG, MPI_COMM_WORLD);	
		//debug_print(debug_level, COMM_LOG, "%s\n", "sent CreateCommunicatorRequest");aisha
	}
}


void sendMyMetadata(char *my_metadata, int len, int source, int num_ngbrs, int *ngbrs_ranks)
{	
	MPI_Status status;
	MPI_Request request;

	for(int i = 0; i < num_ngbrs; i++)
	{
		MPI_Send(my_metadata, len, MPI_BYTE, ngbrs_ranks[i], METADATA_TAG, MPI_COMM_WORLD);
		//debug_print(debug_level, COMM_LOG, "%s\n", "sendMyMetadata");aisha
	}
}

void sendMyResult(char *my_result_set, int len, int source, int num_ngbrs, int *ngbrs_ranks)
{
	MPI_Status status;	
	MPI_Request request;

	if(num_ngbrs != 0)
	{			
		/*MPI_Bcast(my_subQ_result, len, MPI_CHAR, source, NEIGHBORS_WORLD);*/
		for(int i = 0; i < num_ngbrs; i++)
		{
			//time_t startTime = time(NULL);

			// printf("Worker %d started sending its sub-query result of length %d to Worker %d\n", source, len, ngbrs_ranks[i]);
			fflush(stdout);
			
			//debug_print(debug_level, COMM_LOG, "Worker %d started sending its \
				sub-query result of length %d to Worker %d\n",
				//source, len, ngbrs_ranks[i]); aisha

			MPI_Send(my_result_set, len, MPI_CHAR, ngbrs_ranks[i], RESULT_SET_TO_JOIN_TAG, MPI_COMM_WORLD);
			
			//debug_print(debug_level, COMM_LOG, "%s\n", "sendMyResult");aisha

			//time_t endTime = time(NULL);

			//fflush(stdout);
		}

		//printf("Worker %d sent its traffic volume to Proxy", source);
		//MPI_Isend(&len, 1, MPI_INT, PROXY, VOLUME_TAG, MPI_COMM_WORLD, &request);
	}

	else
	{
		/***** send sub-query result (to be unioned) to the Proxy *****/
		//debug_print(debug_level, COMM_LOG, "Worker %d sent its sub-query result of length %d to Proxy\n", source, len);aisha
		MPI_Send(my_result_set, len, MPI_CHAR, PROXY, RESULT_SET_TO_UNION_TAG, MPI_COMM_WORLD);
	}
}

int recvCreateCommunicatorRequest(int *num_ngbrs, int **ngbrs_ranks)
{
	MPI_Status status;
	MPI_Recv(num_ngbrs, 1, MPI_INT, MPI_ANY_SOURCE, NUM_NGBRS_TAG, MPI_COMM_WORLD, &status);

	
	//debug_print(debug_level, COMM_LOG, "recvCreateCommunicatorRequest\n", "");
	if((*ngbrs_ranks = (int *) malloc(sizeof(int) * (*num_ngbrs))) == NULL) {
		systemError("Could not malloc ngbrs_ranks");
//aisha		debug_print(debug_level, ERROR_LOG, "%s\n", "Could not malloc ngbrs_ranks");
		
	}
	MPI_Recv(*ngbrs_ranks, *num_ngbrs, MPI_INT, status.MPI_SOURCE, NGBRS_RANKS_TAG, MPI_COMM_WORLD, &status);
//aisha	debug_print(debug_level, COMM_LOG, "recvCreateCommunicatorRequest\n", "");

//	printf("\tneed to create COMM for %d ngbrs\n", *num_ngbrs);	//aisha	

	return status.MPI_SOURCE;  
}

int sendCreateCommunicatorAck(int *num_ngbrs, int **ngbrs_ranks)
{	
	// Sending acknowledgement for creating new comm
	MPI_Status status;
	char* ackMsg="ACK";
	MPI_Send(ackMsg, strlen(ackMsg), MPI_CHAR, status.MPI_SOURCE, ACK_TAG, MPI_COMM_WORLD);	
	debug_print(debug_level, COMM_LOG, "sendCreateCommunicatorAck\n", "");
	return status.MPI_SOURCE;  
}

void recieveCommunicatorCreationAck(int num_ngbrs, int *ngbrs_ranks, int comm_size, int *comm_ranks)
{
	for(int i = 0; i < num_ngbrs; i++)
	{
		// Receiving acknowledgement for creating new comm
		MPI_Status status;
		int message_len=3;
		char* buf;
		if((buf = (char *)malloc(message_len * sizeof(char))) == NULL) {
			systemError("Can not allocate buffer in URecv");	
			debug_print(debug_level, ERROR_LOG, "%s\n", "Can not allocate buffer in URecv");			
		}
		MPI_Recv(buf, message_len, MPI_CHAR, ngbrs_ranks[i], ACK_TAG, MPI_COMM_WORLD, &status);		
		debug_print(debug_level, COMM_LOG, "recieveCommunicatorCreationAck\n", "");
	}
}		

/**
 * This receives the neighbour results using the above created communicators 
 * via a MPI_Bcast
 */
void *receiveNgbrResult(void *arg)
{  
	int my_rank1;
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank1);

	fflush(stdout);
	//debug_print(debug_level, COMM_LOG, "Worker %d in receiveNgbrResult\n", my_rank1);

	MPI_Status status;
	char error_msg[MPI_MAX_ERROR_STRING];
	int *error_msglen;

	ResultInfo info  = *((ResultInfo *) arg);
	char **ngbr_result_set = info.ngbr_result_set;
	int *msg_len = info.msg_len;

	MPI_Comm NGBRS_COMM = *((MPI_Comm *) info.NGBRS_COMM);
	int ngbr_rank = info.ngbr_rank;
	int my_rank = info.my_rank;
	time_t startTime = time(NULL);
	int comm_size = 0;
	MPI_Comm_size(NGBRS_COMM, &comm_size);

	//debug_print(debug_level, COMM_LOG, "---->THE COMM SIZE IS %d\n", comm_size);

	MPI_Bcast(msg_len, 1, MPI_INT, 0, NGBRS_COMM);
	//debug_print(debug_level, COMM_LOG, "***** Worker %d recvd that the length of the message is %d from Worker %d\n", my_rank, *msg_len, ngbr_rank);

	*ngbr_result_set = (char*) malloc(sizeof(char) * (*msg_len));
	char* new_nr = malloc(sizeof(char)*(*msg_len));
	char buffer[*msg_len];	//this seems unnecessary

	MPI_Bcast(new_nr, *msg_len, MPI_CHAR, 0, NGBRS_COMM);
	//debug_print(debug_level, COMM_LOG, "***** Worker %d recvd the message of length %d from Worker %d\n", my_rank, *msg_len, ngbr_rank);
	time_t endTime = time(NULL);
	//debug_print(debug_level, COMM_LOG, ">>>>>>>>>>>>>>>>>>>>> EXITED\n", "");
	return (void*) new_nr;
}

void createCommunicator(MPI_Comm *NGBRS_COMM, int comm_size, int *comm_ranks)
{
	MPI_Group NGBRS_GROUP, MPI_COMM_GROUP;
	
	MPI_Comm_group(MPI_COMM_WORLD, &MPI_COMM_GROUP);
	
	MPI_Group_incl(MPI_COMM_GROUP, comm_size, comm_ranks, &NGBRS_GROUP);		
	
	MPI_Comm_create_group(MPI_COMM_WORLD, NGBRS_GROUP, NGBRS_COMM_TAG, NGBRS_COMM);
	
}


int hasJoinNeighbor(Node *node)
{
	Neighbor *ngbr;

	for(ngbr = node->out_neighbors; ngbr != NULL; ngbr = ngbr->next)
	{
		if(ngbr->node->isJoinNode) 
		//&& !ngbr->node->is_deleted)
		{
			return 1;
		}
	}

	return 0;
}


void *recievePartialResultsT(void *arg)
{
	PartialResultsStruct pr = *((PartialResultsStruct *)arg);
	
	int my_rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
	
	MPI_Status status;
	recvCreateCommunicatorRequest(pr.comm_size, pr.comm_ranks);
	createCommunicator(pr.NGBRS_COMM,(*pr.comm_size), (*pr.comm_ranks));
	sendCreateCommunicatorAck(pr.comm_size, pr.comm_ranks);
	sendCreateCommunicatorAck(pr.comm_size, pr.comm_ranks);
	MPI_URecv(pr.ngbr_metadata, MPI_CHAR, MPI_ANY_SOURCE, METADATA_TAG, MPI_COMM_WORLD, &status);
	parseMetadataMessage(*pr.ngbr_metadata, pr.ngbr_num_keys, pr.ngbr_keys);
	debug_print(debug_level, COMM_LOG, "Worker %d recvd metadata from Worker %d\n", my_rank, status.MPI_SOURCE);
	free(*pr.ngbr_metadata);		

	// void *exit_status;
	pr.tid2_arg->NGBRS_COMM = pr.NGBRS_COMM;
	
	pr.tid2_arg->ngbr_rank = status.MPI_SOURCE;

	pr.tid2_arg->ngbr_result_set = pr.my_result_set; 
	pr.tid2_arg->msg_len = pr.msg_len; 	

	pthread_create(pr.ngbr_tid, NULL, receiveNgbrResult, pr.tid2_arg);

	if(pr.num_results_recvd == (pr.my_in_degree - 1))
	{
		joinHelper(my_rank, pr.my_num_keys, pr.my_keys, *pr.ngbr_num_keys, *pr.ngbr_keys,  
		*pr.ngbr_result_set, *pr.final_num_keys, *pr.final_keys, *pr.subQ_tid, *pr.ngbr_tid);
	}

	else
	{

		joinHelper(my_rank, pr.my_num_keys, pr.my_keys, *pr.ngbr_num_keys, *pr.ngbr_keys,
		*pr.ngbr_result_set, 0, NULL, *pr.subQ_tid, *pr.ngbr_tid);
	}		
	
}



void recievePartialResults(int *comm_size, int **comm_ranks, MPI_Comm *NGBRS_COMM, 
ResultInfo *tid2_arg, char **ngbr_metadata, short *ngbr_num_keys, char ***ngbr_keys, 
char **my_result_set, pthread_t *ngbr_tid, int *msg_len, int num_results_recvd, int my_in_degree, 
short *my_num_keys, char ***my_keys, char **ngbr_result_set, short *final_num_keys, 
char ***final_keys, pthread_t *subQ_tid) 
{
	int my_rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
	
	MPI_Status status;
	recvCreateCommunicatorRequest(comm_size, comm_ranks);
	createCommunicator(NGBRS_COMM,(*comm_size), (*comm_ranks));
	sendCreateCommunicatorAck(comm_size, comm_ranks);
	sendCreateCommunicatorAck(comm_size, comm_ranks);
	MPI_URecv(ngbr_metadata, MPI_CHAR, MPI_ANY_SOURCE, METADATA_TAG, MPI_COMM_WORLD, &status);
	parseMetadataMessage(*ngbr_metadata, ngbr_num_keys, ngbr_keys);
	debug_print(debug_level, COMM_LOG, "Worker %d recvd metadata from Worker %d\n", my_rank, status.MPI_SOURCE);
	free(*ngbr_metadata);		

	// void *exit_status;
	tid2_arg->NGBRS_COMM = NGBRS_COMM;
	
	tid2_arg->ngbr_rank = status.MPI_SOURCE;

	tid2_arg->ngbr_result_set = my_result_set; 
	tid2_arg->msg_len = msg_len; 	

	pthread_create(ngbr_tid, NULL, receiveNgbrResult, tid2_arg);

	if(num_results_recvd == (my_in_degree - 1))
	{
		joinHelper(my_rank, my_num_keys, my_keys, *ngbr_num_keys, *ngbr_keys,  
		*ngbr_result_set, *final_num_keys, *final_keys, *subQ_tid, *ngbr_tid);
	}

	else
	{

		joinHelper(my_rank, my_num_keys, my_keys, *ngbr_num_keys, *ngbr_keys,
		*ngbr_result_set, 0, NULL, *subQ_tid, *ngbr_tid);
	}	

}



void *recievePartialResultsT2(void *arg)
{
	PartialResultsStruct pr = *((PartialResultsStruct *)arg);
	
	int my_rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
	
	MPI_Status status;
	recvCreateCommunicatorRequest(pr.comm_size, pr.comm_ranks);
	createCommunicator(pr.NGBRS_COMM,(*pr.comm_size), (*pr.comm_ranks));
	sendCreateCommunicatorAck(pr.comm_size, pr.comm_ranks);
	sendCreateCommunicatorAck(pr.comm_size, pr.comm_ranks);
	MPI_URecv(pr.ngbr_metadata, MPI_CHAR, MPI_ANY_SOURCE, METADATA_TAG, MPI_COMM_WORLD, &status);
	parseMetadataMessage(*pr.ngbr_metadata, pr.ngbr_num_keys, pr.ngbr_keys);
	debug_print(debug_level, COMM_LOG, "Worker %d recvd metadata from Worker %d\n", my_rank, status.MPI_SOURCE);
	free(*pr.ngbr_metadata);		

	// void *exit_status;
	pr.tid2_arg->NGBRS_COMM = pr.NGBRS_COMM;
	
	pr.tid2_arg->ngbr_rank = status.MPI_SOURCE;

	pr.tid2_arg->ngbr_result_set = pr.my_result_set; 
	pr.tid2_arg->msg_len = pr.msg_len; 	

	pthread_create(pr.ngbr_tid, NULL, receiveNgbrResult, pr.tid2_arg);

	if(pr.num_results_recvd == (pr.my_in_degree - 1))
	{
		joinHelper(my_rank, pr.my_num_keys, pr.my_keys, *pr.ngbr_num_keys, *pr.ngbr_keys,  
		*pr.ngbr_result_set, *pr.final_num_keys, *pr.final_keys, *pr.subQ_tid, *pr.ngbr_tid);
	}

	else
	{

		joinHelper(my_rank, pr.my_num_keys, pr.my_keys, *pr.ngbr_num_keys, *pr.ngbr_keys,
		*pr.ngbr_result_set, 0, NULL, *pr.subQ_tid, *pr.ngbr_tid);
	}		
	
}
