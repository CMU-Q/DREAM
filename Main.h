#ifndef _MAIN_H_
#define _MAIN_H_

#include <errno.h>
#include <error.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <mpi.h>
#include <getopt.h> 
#include <unistd.h>
//#include <sys/wait.h>
#include "Debug.h"

#include "ProxyUtil.h"
#include "WorkerUtil.h"
#include "MiscUtil.h"
#include "Wrapper.h"
#include "Client.h"
#include "Proxy.h"
#include "Worker.h"

#define PROXY 0
#define CLIENT 1

#define Q_TAG 1
#define Q_RESULT_TAG 2

#define SUBQ_TAG 3
#define METADATA_TAG 4
#define FINAL_KEYS_TAG 5
#define RESULT_SET_TO_JOIN_TAG 6
#define RESULT_SET_TO_UNION_TAG 7
#define ACK_TAG 13

#define NUM_NGBRS_TAG 7
#define NGBRS_RANKS_TAG 8
#define NGBRS_COMM_TAG 9
#define VOLUME_TAG 10

#define TEST_TAG 11



void unionResultSets(char **outer_relation, char *inner_relation);
void MPI_URecv_v2(char **buf, MPI_Datatype datatype, int source, int tag,
                        MPI_Comm comm, MPI_Status *status);

void createCommunicatorHelper(MPI_Comm *NGBRS_WORLD, int source, int num_ngbrs, int *ngbrs_ranks);

void bCastToNgbrs(MPI_Comm NGBRS_COMM, char *my_partial_result, int len, char **my_keys, short my_num_keys,
                                        int source, int num_ngbrs, int *ngbrs_ranks);

void sendToProxy(char *my_partial_result, int len,
                                        int source, int num_ngbrs, int *ngbrs_ranks);

#endif
