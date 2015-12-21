#ifndef _EXTENDEDHASHJOIN_H_
#define _EXTENDEDHASHJOIN_H_

#include <string>
#include <vector>
#include <utility>
#include <iostream>
#include <tbb/concurrent_hash_map.h>
#include <tbb/parallel_for.h>
#include <boost/foreach.hpp>

extern "C"
{
	#include "Errors.h"	
	#include <stdio.h>
	#include <stdlib.h>
	#include "Debug.h"
	typedef unsigned int uint;
	typedef unsigned long ulong;
}
	
struct result_struct 
{
    char *my_result_set;
	char *ngbr_result_set;
};

struct Record
{
        //uint *data;
	ulong *data;
};

typedef tbb::concurrent_hash_map<ulong, std::vector<Record*> > MAP;

struct OuterRelation 
{
	int isEmpty;
	pthread_t tid;
	short num_keys;
	MAP hash_table;
	pthread_t subQ_tid;
	short *join_keys_ndxs;
};


struct InnerRelation
{
	int isEmpty;
	pthread_t tid;
	short num_keys;
	MAP hash_table;
	pthread_t ngbr_tid;
	short *join_keys_ndxs;
};

struct HashTableStruct
{
	MAP hash_table;
};


struct Output
{
	char *filename;
	FILE *stream;
	short num_cols;
	short num_join_keys;
	short *order;
};

struct Args
{
	long chunk_size;
	long offset;
	void *instream;
	FILE *outstream;
	int isInner;
};

void* doOuterRelation(void *resultStruct);

void* doInnerRelation(void *resultStruct);


void joinResultSets (int my_worker_id, short *my_num_keys, char **my_keys[],
              short ngbr_num_keys, char *ngbr_keys[], char *ngbr_result_set,
	      short final_num_keys, char *final_keys[], pthread_t subQ_tid, pthread_t ngbr_tid);

void mergeKeys(char **my_keys[], char *ngbr_keys[], short final_num_keys, char *final_keys[]);
void findJoinKeys (char *keys1[], char *keys2[]);
short countJoinKeys (char *keys1[], char *keys2[]);
int isEmpty (char *result_set);
void finalizeJoin ();
void initDatastructures ();
void clean ();

void *probeRelation (char *buffer);
void *partitionRelation (char *relation);

void *probeRelationHelper (void *arg);
void *partitionRelationHelper (void *arg);

void bubbleSort (char *elems[], int num_elems);
void matchRelations ();

void matchRecords (std::vector<Record*> list1, std::vector<Record*> list2);
void matchRecord (Record* record1, Record* record2, FILE *stream);
void writeRecord (Record* record1, Record* record2, FILE *stream);
Record *createNewRecord (short num_keys);
void probe (Record *record1, ulong key, FILE *stream);
void freeRecord (Record **record);
void getUsage (char *task);

#endif
