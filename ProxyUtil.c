#include "ProxyUtil.h"

/**
 * This is a set of helper functions used by the PROXY. After the Query is read
 * the sendSubQToWorker function is used to send the respective sub-query to 
 * the node/machine.
 */

extern int debug_level;

int8_t *createFinalKeysMessage (Query *Q, int *message_len)
{
	int8_t *message, *m;
	int16_t num_results;
	
	int header_len = sizeof(int16_t);
	int trailer_len = sizeof(int8_t);
	int payload_len = 0;

	for(Result *res = Q->results; res != NULL; res = res->next)
	{
		payload_len += strlen(res->variable) + 1;
	}
	
	*message_len = header_len + payload_len + trailer_len;

	if(!(message = (int8_t *) malloc(sizeof(int8_t) * (*message_len)))) {
		applicationError("Could not allocate message in createFinalKeysMsg");
		debug_print(debug_level, ERROR_LOG, "%s\n", "Could not allocate message in createFinalKeysMsg");
	}
	
	m = message;

	num_results = htons(Q->num_results);
	memcpy(m, (int8_t *)&num_results, 2);   m += 2;
	
	for(Result *res = Q->results; res != NULL; res = res->next)
	{
		int result_len = strlen(res->variable);

		memcpy(m, (int8_t *)res->variable, result_len);  
		m += result_len;
		
		memcpy(m, (int8_t *)" ", 1);
		m += 1;	
	}

	memcpy(m, (int8_t *)"\0", 1);

	return message;
}

int8_t *createSubQMessage(Node *node, int *message_len)
{
	int16_t in_degree = 0, out_degree = 0, num_results;
	Neighbor *out_ngbr, *in_ngbr;
	int8_t *message, *m;		

	for(in_ngbr = node->in_neighbors; in_ngbr != NULL; in_ngbr = in_ngbr->next)
	if(in_ngbr->node->subQ != NULL) //&& !in_ngbr->node->is_deleted)	
		in_degree++;

	for(out_ngbr = node->out_neighbors; out_ngbr != NULL; out_ngbr = out_ngbr->next)
	if(out_ngbr->node->subQ != NULL) //&& !in_ngbr->node->is_deleted)
		out_degree++;

	int header_len  = sizeof(int16_t) + sizeof(int16_t) + (sizeof(int32_t) * out_degree) + sizeof(int16_t);
	int payload_len = strlen(node->subQ->qstring);
	int trailer_len = sizeof(int8_t);
	
	*message_len = header_len + payload_len + trailer_len; 
	
	if(!(message = (int8_t *) malloc(sizeof(int8_t) * (*message_len)))) {
		applicationError("Could not allocate message in createSubQMessage");
		debug_print(debug_level, ERROR_LOG, "%s\n", "Could not allocate message in createSubQMessage");
	}

	m = message;

	in_degree   = htons(in_degree);
	out_degree  = htons(out_degree);
	num_results = htons(node->subQ->num_results);

	memcpy(m, (int8_t *)&in_degree, 2);   m += 2; 		
	memcpy(m, (int8_t *)&out_degree, 2);  m += 2;
	
	for(out_ngbr = node->out_neighbors; out_ngbr != NULL; out_ngbr = out_ngbr->next)
	{	
		if(out_ngbr->node->subQ != NULL) //&& !out_ngbr->node->is_deleted)
		{
			int32_t out_ngbr_rank = htonl(out_ngbr->node->worker_rank);
			memcpy(m, (int8_t *)&out_ngbr_rank, 4);  m += 4;
		}
	}				

	memcpy(m, (int8_t *)&num_results, 2); m += 2;
	memcpy(m, (int8_t *)node->subQ->qstring, payload_len); m += payload_len;
	memcpy(m, (int8_t *)"\0", 1); 

	return message;
}

char *readFile(char *filename)
{
	FILE *fd;
	long fsize;
	char *query;

	if((fd = fopen(filename, "r")) == NULL) {
		systemError("Could not open file in readFile");
		debug_print(debug_level, ERROR_LOG, "%s\n", "Could not open file in readFile");
	}
 
	fseek(fd, 0, SEEK_END);
	fsize = ftell(fd);
	fseek(fd, 0, SEEK_SET);

	if(!(query = malloc(sizeof(char) * (fsize + 1)))) {
		systemError("Could not allocate buffer in readFile");
		debug_print(debug_level, ERROR_LOG, "%s\n", "Could not allocate buffer in readFile");
	}

	fread(query, sizeof(char), fsize, fd);
	fclose(fd);

	query[fsize] = '\0';

	return query;
}

/*
	Sends subQuery to respective worker
*/
void sendSubQToWorker(Query *Q, Node *node, int *num_workers_to_reply)
{
	char *msg;
	int msg_len;
	int worker = node->worker_rank;
	msg = createFinalKeysMessage(Q, &msg_len);
	MPI_Send(msg, msg_len, MPI_BYTE, worker, FINAL_KEYS_TAG, MPI_COMM_WORLD);
	free(msg);
	msg = createSubQMessage(node, &msg_len);

	//if(!hasJoinNeighbor(node))
	//{
	//	(*num_workers_to_reply)++;
	//}

	if(node->out_degree == 0)
		(*num_workers_to_reply)++;

	//printf("PROXYUTIL: this is the num workers who should reply: %d\n", *num_workers_to_reply);	
	MPI_Send(msg, msg_len, MPI_BYTE, worker, SUBQ_TAG, MPI_COMM_WORLD);
	free(msg);
}
