#ifndef _QUERY_H_
#define _QUERY_H_

#include "Errors.h"
#include "OptimizerWrapper.h"
#include "Debug.h"

#define NUM_WORKERS 3
#define MIN_RANK 2
#define MAX_RANK (MIN_RANK + NUM_WORKERS - 1)

#define AVAILABLE 1

void loadRDF();
int flip();

Query *createNewQuery();
SubQuery *createNewSubQuery();
void createSubQueryStr(Node *n);
void initQueryPlanner ();

void identifyJoinNodes(Query *Q);
Query *planQuery(char *query);
//void executeSubQuery(char *query, int worker_id);
void *executeSubQuery(void *arg);
char *extractToken(char **query);
void extractResults(Query *Q, char **query);
void printResults(Query *Q);
void addResult(Result **list, Result **element);

void extractConditions(Query *Q, char **query);
void extractCondition(Query *Q, char **query);
void addCondition(Condition **list, Condition **element);
void printConditions(Query *Q);
void printQuery(Query *Q);
void printSubQueries(Query *Q);
void printSubQuery(SubQuery *subQ);

void createGraph(Query *Q);
Node *findNode(Node *nodes, char *label);
void addNode(Node **nodes, Node **node);
void addNeighbor(Neighbor **neighbors, Neighbor **neighbor);
void printGraph(Node *nodes);
void printNeighbors(Neighbor *neighbors);
Node *createNewNode(char *label);
Neighbor *createNewNeighbor(Node *node);

void assignConditionsToNodes(Query *Q);
void assignCondsToNodes_Q9(Query *Q);
void assignCondsToNodes_CQ(Query *Q);
void assignCondsToNodes_Q2(Query *Q);
void assignCondsToNodes_newQ9(Query *Q);

void addChosenResult(ChosenResult **list, ChosenResult **element);
void addChosenCondition(ChosenCondition **list, ChosenCondition **element);
ChosenResult *findResult(ChosenResult *results, char *variable);
void selectWorkers(Query *Q);
short degree(Node *n);

void assignCondition(Node **n, Condition *c, short isX);
void assignSubQueries(Query *Q);

int  addResult_p(ChosenResult **list, char *str);
void addCondition_p(ChosenCondition **list, Condition *cond);

Neighbor *removeNeighbor(Neighbor **neighbors, char *label);
Neighbor *findNeighbor(Neighbor *ngbrs, char *label);
void addSubQuery(SubQuery **subQueries, SubQuery **subQ);

void createSubQStr(Node *node);
void createResultsStr(Node *node, char *existing_results[], short *num_results, char **buf, int level);
void createConditionsStr(Node *node, char **buf);
int resultExists(char *existingr_results[], char *new_result);
void assignNodeToWorker(Node *node, short status[]);
void bubbleSort (char *elems[], int num_elems);
void combineSubQStrs(Node *node, Node **ngbr);
void printShareds(Query *Q);
void printExclusives(Query *Q);
void printSupersets(Query *Q);
char *read_file(char *filename);


#endif

