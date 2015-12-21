#ifndef _STRUCTS_H_
#define _STRUCTS_H_

#include <stdint.h>
#include <float.h>

extern short QID;

// extern const char *DB;
// extern const char *QUERIES;
// extern const char *STAT_FILES;

extern char STAT_FILES[100];;
extern char QUERIES[100];
extern char DB[100];
extern int query_ndx;
   
extern char QUERY_FILE[100];
extern char RESULT_FILE[100];
extern char STAT_FILE_1[100];
extern char STAT_FILE_2[100];

extern int DESIRED_PLAN_NDX;
extern int SHOULD_GET_NETWORK_SPEED;
extern long double networkSpeed;

typedef uint64_t ulong;

typedef struct result
{
	short exists;
	char *variable;
	struct result *next;

} Result;


typedef struct condition
{
	short id;
	short isExclusive;
	char *subj, *pred, *obj;
	struct condition *next;

} Condition;

typedef struct node
{	
	short nid;
	char *label;
	struct neighbor *in_neighbors;
	struct neighbor *out_neighbors;
	struct neighbor *merged_ngbrs;
	struct neighbor *nextRecipientNgbr;
	struct node *next;
	short in_degree;
	short out_degree;
	short isJoinNode;
	short is_deleted;
	short worker_rank;
	struct subquery *subQ;
	struct node **optimalCompactGraph;

} Node;

typedef struct neighbor
{
	Node *node;
	short isMerged;
	struct neighbor *next;

} Neighbor;

typedef struct query
{
	short id;
	Result *results;
	Condition *conditions;
	char *filter;
	short num_nodes;
	short num_results;
	short num_conditions;
	short num_join_nodes;
	short num_merged_nodes;
	long estimateCost;
	Node **optimalPlan;
	Node **compactGraph;
	Node *graph;

} Query;

typedef struct chosenresult
{
	Result *result_p;
	char *variable;
	struct chosenresult *next;

} ChosenResult;

typedef struct chosencondition
{
	Condition *condition_p;
	struct chosencondition *next;

} ChosenCondition;

typedef struct subquery
{
	ChosenResult *results;
	ChosenCondition *shareds;
	ChosenCondition *superset;
	ChosenCondition *conditions;
	ChosenCondition *optimalset;
	ChosenCondition *exclusives;
	char *hashkey;
	char *filter;
	char qstring[2000];
	short num_results;
	short num_conditions;
	short num_exclusives;
	short num_shareds;
	
	// ulong resultSize;        /* cost of the optimal set */
	// ulong scanSize;
	// ulong hashJoinCost;
	// ulong commCost;
	// ulong scanCost;
	// ulong cummCost;
	long double resultSize;        /* cost of the optimal set */
	long double scanSize;
	long double hashJoinCost;
	long double commCost;
	long double scanCost;
	long double cummCost;
	
	struct subquery *next;

} SubQuery;

typedef struct subq_info
{
	char *subQ;
	int w_rank;

} SubQInfo;

typedef struct result_info
{	
	char **ngbr_result_set;
	void * NGBRS_COMM;
	int ngbr_rank;
	int *msg_len;
	int my_rank;

} ResultInfo;	

#endif
