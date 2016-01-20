#ifndef _PLAN_COST_ESTIMATOR_H_
#define _PLAN_COST_ESTIMATOR_H_

#include <string>
#include <vector>
#include <cstring>
#include <utility>
#include <iostream>
#include <map>
#include <tbb/concurrent_hash_map.h>
#include <boost/unordered_map.hpp>
#include <boost/foreach.hpp>

//#include "Errors.h"
//#include "Structs.h"
//#include "QueryPlanner.h"

extern "C"
{
	#include "Debug.h"
	#include "Errors.h"
	#include "Structs.h"
	#include "QueryPlanner.h"
};

#define MAX(x, y) (((x) > (y)) ? (x) : (y))

struct my_sz_cmp
{
    bool operator()(const char* a, const char* b) const
    {
        if (!(a && b))
            return a < b;
        else
            return strcmp(a, b) < 0;
    }
};

#define SOME_LARGE_VALUE ((ulong)(-1ULL))
#define COMM_COST_PER_PAGE 5
#define AVG_SIZE_OF_TUPLE 6
#define IO_COST_PER_PAGE 1
#define PAGE_SIZE (4 * 1024)

typedef ChosenCondition CCond;
//typedef std::map<char const*, ulong, my_sz_cmp> MAP;
typedef std::map<char const*, long double, my_sz_cmp> MAPDOUBLE;

void printScanSizeStatsTable();
void printResultSizeStatsTable();

/**Runtime and fileSize**/
void computeGraphCost_v2 (Query *Q, Node *startNode, long double *estimateCost, int level);
long double estimateTotalPlanCost (Query *Q, Node *compactGraph[]);
long double costOfHashJoinBetween (Node *node, Neighbor *ngbr);

long double costOfHashJoinBetween (Node *node, Node *ngbr);
long double getResultSize (Node *node, char *subQkey, Query *Q, Node *starterNode);
void getSubQScanStatistics(char *query);
long double getScanSize (Node *joinNode, char *subQKey, Query *Q, Node *starterNode);

long double getSubQResultSize(char *query, char *subQKey);


/**scanSize and ResultSize**/
//ulong computeGraphCost_v2 (Node *starterNode, int level);
/*void computeGraphCost_v2 (Query *Q, Node *startNode, ulong *estimateCost, int level);
void computeGraphCost (Node *startNode, ulong *estimateCost, int level);
ulong estimateTotalPlanCost (Query *Q, Node *compactGraph[]);
ulong costOfHashJoinBetween (Node *node, Neighbor *ngbr);

ulong costOfHashJoinBetween (Node *node, Node *ngbr);
ulong getResultSize (Node *node, char *subQkey, Query *Q, Node *starterNode);
ulong getScanSize (Node *joinNode, char *subQKey, Query *Q, Node *starterNode);

unsigned long getSubQResultSize(char *query);
unsigned long getSubQScanSize(char *query);
*/

void generateSubQueryKey (Query *Q, Node *node, char **buf);
char *combineSubQKeys (Node *node, Node *ngbr);
void combineSubQStrs (Query *Q, Node *node, Node *ngbr);
void loadResultSizeStats (const char *filename);
void loadScanSizeStats (const char *filename);
int hasJoinNeighbor(Node *node);


#endif
