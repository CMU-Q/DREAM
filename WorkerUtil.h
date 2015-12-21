#ifndef _WORKER_UTIL_H_
#define _WORKER_UTIL_H_

#include "QueryPlanner.h"
#include "Main.h"

//int createResultMessage(char **message, int worker_id, char *results[], short num_results);
//void parseResultMessage(char *message, short *num_results, char **results[], char **result_set);

//void parseSubQMessage(char *message, short *in_degree, short *out_degree, int **ranks,
//			           short *num_results, char **results[], char **subQ);

//void parseFinalKeysMessage(char *message, short *final_num_results, char **final_results[]);

typedef struct partial_results_struct
{
	int *comm_size;
	int **comm_ranks;
	MPI_Comm *NGBRS_COMM;
	ResultInfo *tid2_arg;
	char **ngbr_metadata;
	short *ngbr_num_keys;
	char ***ngbr_keys; 
	char **my_result_set;
	pthread_t *ngbr_tid;
	int *msg_len;
	int num_results_recvd;
	int my_in_degree;
	short *my_num_keys;
	char ***my_keys; 
	char **ngbr_result_set;
	short *final_num_keys;
	char ***final_keys;
	pthread_t *subQ_tid;
} PartialResultsStruct;

void parseFinalKeysMessage(char *message, short *final_num_keys, char **final_keys[]);

void parseSubQMessage(char *message, short *in_degree, short *out_degree, int **ranks,
                                   short *num_keys, char **keys[], char **subQ);

int createMetadataMessage(char **message, char *keys[], short num_keys);

int createResultMessage(char **message, int worker_id);

void parseMetadataMessage(char *message, short *num_keys, char **keys[]);

void parseResultMessage(char *message, char **result_set);

int hasJoinNeighbor(Node *node);
void sendMyMetadata(char *my_metadata, int len, int source, int num_ngbrs, int *ngbrs_ranks);
void sendMyResult(char *my_subQ_result, int len, int source, int out_degree, int *out_neighbors_ranks);
void createCommunicatorRanksArray(int my_rank, int *my_ngbrs_ranks, int my_num_ngbrs, 
												int *comm_size, int **comm_ranks);
void *receiveNgbrResult(void *arg);

void sendCreateCommunicatorRequest(int num_ngbrs, int *ngbrs_ranks, int comm_size, int *comm_ranks);
void recieveCommunicatorCreationAck(int num_ngbrs, int *ngbrs_ranks, int comm_size, int *comm_ranks);

int recvCreateCommunicatorRequest(int *num_ngbrs, int **ngbrs_ranks);
int sendCreateCommunicatorAck(int *num_ngbrs, int **ngbrs_ranks);

void createCommunicator(MPI_Comm *NGBRS_COMM, int num_ngbrs, int *ngbrs_ranks);

void recievePartialResults(int *comm_size, int **comm_ranks, MPI_Comm *NGBRS_COMM, 
			ResultInfo *tid2_arg, char **ngbr_metadata, short *ngbr_num_keys, char ***ngbr_keys, 
			char **my_result_set, pthread_t *ngbr_tid, int *msg_len, int num_results_recvd, int my_in_degree, 
			short *my_num_keys, char ***my_keys, char **ngbr_result_set, short *final_num_keys, 
			char ***final_keys, pthread_t *subQ_tid);
		
void *recievePartialResultsT(void *arg);	

void *recievePartialResultsT2(void *arg);

	
#endif
