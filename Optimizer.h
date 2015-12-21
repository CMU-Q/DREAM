#ifndef _OPTIMIZER_H_
#define _OPTIMISER_H_

#include "PlanCostEstimator.h"
#include "Debug.h"
		
#define KILO 1024
		
void initOptimizer (Query *Q);
void loadStatsHelper (char *filename);
void getOptimalPlanHelper (Query **Q);

void printAllEnumeratedSets(Query *Q); //aisha
void enumerateAllSets (Query *Q);
void printSets (Node *n, short slot);//aisha
void enumerateSets (Node *n, short slot);
void createSet (int size, CCond **new_set, CCond *list, CCond *elem);

void createCompactGraph_v2 (Query **Q);
void createCompactGraph (Query **Q);
void mergeCompactGraph (Query *Q);

Node **buildNewCompactGraph (Node *compactGraph[], int numJoinNodes,
                        int ndxOfRecipientNode, int ndxOfMergedNode);

void copyCompactGraph  (Node **newCompactGraph[], Node *oriCompactGraph[],
                   		int numJoinNodes, int ndxOfMergedNode);

void mergeCompactGraphRecuresively (Query *Q, Node *compactGraph[], int level, int ndx);
int isValidCompactPlan (Query *Q, Node *compactGraph[]);
void traverseMergedNodes (Node *node, Node *compactGraph[], int *mergedNodes);

void copyNeighbors (Neighbor **dest, Neighbor *src);
void findOptimalCompactGraph (Query *Q);
int getNextPlan ();
void computePlanCost ();
int isValidPlan (Query *Q);
void assignOptimalPlan (Query *Q);
void loadStats (char *filename);

void init (Query *Q);
void printAllSets ();
void printOptimalPlan ();
void printSet (CCond *set);
void printCompactGraph (Node *compactGraph[], int num_join_nodes);
void printNgbrs (Neighbor *neighbors);
void cleanDatastructures();

void updateCompactGraph (Query **Q);
void setOptimalPlan (Query **Q);
void estimatePlanCost (Query *Q);
void temp (Query *Q);


void calculateNetworkSpeed();
#endif

