#include "PlanCostEstimator.h"

//static MAP resultSizeStatsTable;
//static MAP scanSizeStatsTable;
static MAPDOUBLE scanSizeStatsTable;
static MAPDOUBLE resultSizeStatsTable;

extern "C" int debug_level;

long double estimateTotalPlanCost (Query *Q, Node *compactGraph[])
{
	
	long double estimateCost = 0;
	Node *joinNode, *starterNode = NULL;
	

	//debug_print(debug_level, Q_PLAN_LOG,"%s\n", "Estimating the cost of the above plan");
	
	/*for each join node, find the result size of its subQ*/
	for(int i = 0; i < Q->num_join_nodes; i++)
	{	

		joinNode = compactGraph[i];

		char hashkey[60], *ptr;
		memset(hashkey, '\0', 60);
		ptr = hashkey;

		generateSubQueryKey(Q,joinNode,&ptr);
		
		joinNode->subQ->hashkey = 
		(char*)(malloc(sizeof(char) * (strlen(hashkey)+1)));
		
		strcpy(joinNode->subQ->hashkey, hashkey);
		
		//printf("SubQKey: %s\n", joinNode->subQ->hashkey); 
		
		joinNode->subQ->resultSize = getResultSize
		(joinNode, joinNode->subQ->hashkey, NULL, NULL);

		if((long double) joinNode->subQ->resultSize < 0)
			{return -1;}

		joinNode->subQ->scanSize = getScanSize
		(joinNode, joinNode->subQ->hashkey, NULL, NULL);
		
		debug_print(debug_level, Q_PLAN_LOG,
		"Join Node %s KEY: %s ResultSize = %Lf, ScanSize = %Lf\n",
		joinNode->label, joinNode->subQ->hashkey, joinNode->subQ->resultSize,
		joinNode->subQ->scanSize);
	}

	/* reset the various costs assigned to a join node
	during the previous computations of the graph's cost */
	for(int i = 0; i < Q->num_join_nodes; i++)
	{
		joinNode = compactGraph[i];
		joinNode->subQ->commCost = 0;
		joinNode->subQ->scanCost = 0;
		joinNode->subQ->cummCost = 0;
		joinNode->subQ->hashJoinCost = 0;
	}
	
	/* compute the cost of the whole graph starting from each
	starter node, i.e. any node whose in degree is zero */
	for(int i = 0; i < Q->num_join_nodes; i++)
	{
		joinNode = compactGraph[i];
		
		if(joinNode->is_deleted
		|| joinNode->in_degree)
		{
			continue;
		}

		starterNode  = joinNode;
		//estimateCost = MAX(estimateCost, 
		//computeGraphCost_v2(starterNode, 0));

		computeGraphCost_v2(Q, starterNode, &estimateCost, 0);
	}

	if(!starterNode)
		{return LDBL_MAX;}
		//{return SOME_LARGE_VALUE;}
	
	return estimateCost;
}

/* DFS to compute cost of each node*/
void computeGraphCost_v2 (Query *Q, Node *starterNode, long double *estimateCost, int level)
{
	long double commCost, scanCost, hashJoinCost, cummCost;
	long double tempCommCost = 0;

	//aisha debug_print(debug_level, Q_PLAN_LOG,
	//aisha "======================== Node %s, Level %d ===================\n",
	//aisha starterNode->label, level);
	
	if(level++ == 0)	//base case
	{
		scanCost  = starterNode->subQ->scanSize;	
		commCost  = starterNode->subQ->resultSize;
		//aisha printf("\t result size: %Lf\n", starterNode->subQ->resultSize);
		
		/*start accurate comm size code -AISHA - storing fileSize in resultSize
		char resultfile[1024];
		ulong fsize;
		FILE *stream;
	
		sprintf(resultfile, "%hd-%s.txt", query_ndx, starterNode->subQ->hashkey);
		
		if(!(stream = fopen(resultfile, "r")))
			printf("Could not open temp rdf result file\n");
		
		fseek(stream, 0L, SEEK_END);
		fsize = ftell(stream);
		fsize = (sizeof(int8_t) * fsize);
		fclose(stream);
		
		printf("\tsize of file %s: %lu\n", resultfile, fsize);
		/*end accurate comm size code*/
		
		/*start new commCost*/
		//commCost = fsize; -AISHA - commCost directly stores fileSize
		tempCommCost = commCost;
//		printf("tempCommCost = %Lf\n", tempCommCost);
		tempCommCost = ceill(tempCommCost/networkSpeed);
//		printf("cost to transmit file = %Lf\n", tempCommCost);
		commCost = tempCommCost;
		/*end new commCost*/
	
		cummCost  = scanCost + commCost;
		
		starterNode->subQ->scanCost = scanCost;
		starterNode->subQ->commCost = commCost;
		starterNode->subQ->cummCost = cummCost;
		
		//if startNode is also an end node, no intermediate data communication
		if(!starterNode->out_degree){	//aisha
			starterNode->subQ->cummCost = scanCost;
		}
	}
	
	/*debug_print(debug_level, Q_PLAN_LOG,"Node %s, cummCost %lu\n", 
	starterNode->label, starterNode->subQ->cummCost);*/ //aisha
	
	/*debug_print(debug_level, Q_PLAN_LOG,
	"Node %s, scanCost %lu, resultSize %lu, commCost %lu, cummCost %lu\n",
	starterNode->label, starterNode->subQ->scanSize,
	starterNode->subQ->resultSize, starterNode->subQ->commCost,
	starterNode->subQ->cummCost); *///aisha
	
	if(!starterNode->out_degree){	//no outgoing neighbours
		*estimateCost = starterNode->subQ->cummCost;
		
		// debug_print(debug_level, Q_PLAN_LOG,
		// "Node %s has no outgoing neighbours. Estimated Cost = %lu\n",
		// starterNode->label, starterNode->subQ->cummCost); //aisha
	 }

	for(Neighbor *ngbr = starterNode->out_neighbors;
	ngbr != NULL; ngbr = ngbr->next)
	{
		scanCost  = ngbr->node->subQ->scanSize;
		cummCost  = ngbr->node->subQ->cummCost;
		
		//aisha debug_print(debug_level, Q_PLAN_LOG,
		// "Node %s (ngbr of Node %s), scanCost %Lf, cummCost %Lf\n",
		// ngbr->node->label, starterNode->label, scanCost, cummCost);
			
		// (cummCost) ? 
		//printf("cummCost: %Lf", cummCost);
		 //printf("MAX(cummCost: %Lf, starterNode->subQ->cummCost: %Lf)\n", cummCost, starterNode->subQ->cummCost);
		 //printf("MAX(scanCost: %Lf, starterNode->subQ->cummCost: %Lf)\n", scanCost, starterNode->subQ->cummCost);
		
		
		cummCost  = (cummCost) ? 
			MAX(cummCost, starterNode->subQ->cummCost):
				MAX(scanCost, starterNode->subQ->cummCost);
		
		
		// debug_print(debug_level, Q_PLAN_LOG,
		// "Node %s new cummCost: %Lf\n", ngbr->node->label, cummCost);
	
		hashJoinCost = costOfHashJoinBetween(starterNode,ngbr);
		cummCost += hashJoinCost;
		
		debug_print(debug_level, Q_PLAN_LOG,
		"Node %s (ngbr) hash join cost with Node %s= %Lf. new cummCost: %Lf\n",
		ngbr->node->label, starterNode->label, hashJoinCost, cummCost);
	
		if(ngbr->node->out_degree)	//add cost of communicating with neighbours
		{

			/* update all the numbers that need updating in 
			the ngbr node after its virtual hash join with 
			the starter node */
			
			ngbr->node->subQ->hashkey = combineSubQKeys
			(starterNode, ngbr->node);

			ngbr->node->subQ->resultSize = getResultSize
			(ngbr->node, ngbr->node->subQ->hashkey, Q, starterNode);

			ngbr->node->subQ->scanSize = getScanSize
			(ngbr->node, ngbr->node->subQ->hashkey, Q, starterNode);	
		
			// printf("computeGraphCost_v2 KEY: %s ResultSize = %Lf, ScanSize = %Lf\n",
			// ngbr->node->subQ->hashkey, ngbr->node->subQ->resultSize);
			
			/*debug_printf(debug_level, Q_PLAN_LOG,
			"KEY: %s ResultSize = %Lf, ScanSize = %Lf\n",
			ngbr->node->subQ->hashkey, ngbr->node->subQ->resultSize,
			ngbr->node->subQ->scanSize);*/
		
			// printf("Node %s (ngbr) KEY: %s ResultSize = %Lf, ScanSize = %Lf\n",
			// ngbr->node->label, ngbr->node->subQ->hashkey,
			// ngbr->node->subQ->resultSize, ngbr->node->subQ->scanSize);

			commCost  = ngbr->node->subQ->resultSize;
			// printf("\t result size: %Lf\n", ngbr->node->subQ->resultSize);
			
			/*start accurate comm size code -AISHA - storing fileSize in resultSize
			char resultfile[1024];
			ulong fsize;
			FILE *stream;
		
			sprintf(resultfile, "%hd-%s.txt", query_ndx, ngbr->node->subQ->hashkey);
			
			if(!(stream = fopen(resultfile, "r")))
				printf("Could not open temp rdf result file\n");
			
			fseek(stream, 0L, SEEK_END);
			fsize = ftell(stream);
			fsize = (sizeof(int8_t) * fsize);
			fclose(stream);
	
			printf("\tsize of file %s: %lu\n", resultfile, fsize);
			/*end accurate comm size code*/
	
			/*start new commCost*/
			//commCost = fsize; -AISHA - commCost directly stores fileSize
			tempCommCost = commCost;
			tempCommCost = ceill(tempCommCost/networkSpeed);
			commCost = tempCommCost;
		//	printf("\tnewCommCost: %Lf\n", commCost);
			/*end new commCost*/
			
			
			cummCost += commCost;
		}
		
		// debug_print(debug_level, Q_PLAN_LOG,
		// "Computed costs for Ngbr %s: scanCost: %Lf, commCost %Lf, cummCost %Lf, hashJoinCost %Lf\n", 
		// ngbr->node->label, scanCost, commCost, cummCost, hashJoinCost);
		
		
		/* remember the computed costs for ngbr node */
		ngbr->node->subQ->scanCost = scanCost;
		ngbr->node->subQ->commCost = commCost;
		ngbr->node->subQ->cummCost = cummCost;
		ngbr->node->subQ->hashJoinCost = hashJoinCost;
		
	}
	
	for(Neighbor *ngbr = starterNode->out_neighbors;
				ngbr != NULL; ngbr = ngbr->next)
	{
		computeGraphCost_v2(Q, ngbr->node, estimateCost, level); //aisha
	}

}

/* DFS to compute cost of each node*/
void computeGraphCost (Node *starterNode, ulong *estimateCost, int level)
{
	long double hashJoinCost = 0, cost1 = 0, cost2 = 0;
	long double commCost = 0, scanCost = 0;

	/* scanSize should be replaced by cost */
	scanCost  = starterNode->subQ->scanSize;	
	starterNode->subQ->scanCost = scanCost;

	if(level == 0)
	*estimateCost += scanCost;	

	commCost  = starterNode->subQ->resultSize;
	commCost *= AVG_SIZE_OF_TUPLE;
	commCost /= PAGE_SIZE;
	commCost *= COMM_COST_PER_PAGE;

	starterNode->subQ->commCost = commCost;
	*estimateCost += commCost;

	for(Neighbor *ngbr = starterNode->out_neighbors;
	ngbr != NULL; ngbr = ngbr->next)
	{
		if(!ngbr->node->isJoinNode) { continue; }

		scanCost  = ngbr->node->subQ->scanSize;
		ngbr->node->subQ->scanCost = scanCost;

		cost1 = (starterNode->subQ->hashJoinCost == 0) ?
		starterNode->subQ->scanCost   :
		starterNode->subQ->hashJoinCost;
		
		cost1 += starterNode->subQ->commCost;
		
		cost2 = (ngbr->node->subQ->hashJoinCost == 0) ?
		labs(ngbr->node->subQ->scanCost - cost1)  :
		labs(ngbr->node->subQ->hashJoinCost - cost1);

		*estimateCost += cost2;

		hashJoinCost = costOfHashJoinBetween
		(starterNode,ngbr);

		ngbr->node->subQ->hashJoinCost = hashJoinCost;

		//*estimateCost += hashJoinCost;

		ngbr->node->subQ->hashkey = combineSubQKeys
		(starterNode, ngbr->node);

		//ngbr->node->subQ->resultSize = getResultSize
		//(ngbr->node, ngbr->node->subQ->hashkey);

		//ngbr->node->subQ->scanSize = getScanSize
		//(ngbr->node, ngbr->node->subQ->hashkey);

		//computeGraphCost(ngbr->node, estimateCost, ++level);
	}

	for(Neighbor *ngbr = starterNode->out_neighbors;
	ngbr != NULL; ngbr = ngbr->next)
	computeGraphCost(ngbr->node, estimateCost, ++level);

}

long double costOfHashJoinBetween (Node *node, Neighbor *ngbr)
{
	long double M, N;
	long double tempM, tempN;
	
	M  = node->subQ->resultSize;
	//aisha printf("\t result size: %Lf\n", node->subQ->resultSize);
	
	/*start accurate result size code -AISHA - storing fileSize in resultSize
	char resultfileM[1024];
	ulong fsizeM;
	FILE *streamM;

	sprintf(resultfileM, "%hd-%s.txt", query_ndx, node->subQ->hashkey);

	if(!(streamM = fopen(resultfileM, "r")))
		printf("Could not open temp rdf result file\n");

	fseek(streamM, 0L, SEEK_END);
	fsizeM = ftell(streamM);
	fsizeM = (sizeof(int8_t) * fsizeM);
	fclose(streamM);

	printf("\tsize of file %s: %lu\n", resultfileM, fsizeM);
	/*end accurate result size code*/
	/**
	M *= AVG_SIZE_OF_TUPLE;
	//M /= PAGE_SIZE; - AISHA - consider changing this back for large datasets
	tempM = M;
	//aisha printf("tempM = %Lf\n", tempM);
	**/
	//M = fsizeM; - AISHA M directly stores fileSize
	tempM = M;
//	printf("tempM %Lf\n", tempM);
	tempM /= PAGE_SIZE;
//	printf("tempM/= PAGE_SIZE = %Lf\n", tempM);
	tempM = ceill(tempM);
	//aisha printf("ceill(tempM) = %Lf\n", tempM);
	M = tempM;
	M *= IO_COST_PER_PAGE;

	
	N  = ngbr->node->subQ->resultSize;
	// printf("\t result size: %lu\n", ngbr->node->subQ->resultSize);
	
	/*start accurate result size code -AISHA  storing fileSize in resultSize
	char resultfile[1024];
	ulong fsize;
	FILE *stream;

	sprintf(resultfile, "%hd-%s.txt", query_ndx, ngbr->node->subQ->hashkey);

	if(!(stream = fopen(resultfile, "r")))
		printf("Could not open temp rdf result file\n");

	fseek(stream, 0L, SEEK_END);
	fsize = ftell(stream);
	fsize = (sizeof(int8_t) * fsize);
	fclose(stream);

	printf("\tsize of file %s: %lu\n", resultfile, fsize);
	/*end accurate result size code*/
	/**
	N *= AVG_SIZE_OF_TUPLE;
	// N /= PAGE_SIZE;	- AISHA - consider changing this back for large datasets
	tempN = N;
	// printf("tempN = %Lf\n", tempN);
	**/
	//N = fsize; - N directly stores fileSize
	tempN = N;
	tempN /= PAGE_SIZE;
//	printf("tempN/= PAGE_SIZE = %Lf\n", tempN);
	tempN = ceill(tempN);
	// printf("ceill(tempN) = %Lf\n", tempN);
	N = tempN;
	N *= IO_COST_PER_PAGE;

	return 3 * (M + N);
}



/** START RUN TIME FUNCTIONS **/
long double getResultSize (Node *joinNode, char *subQKey, Query *Q, Node *starterNode)
{
	MAPDOUBLE::const_iterator iter;
	
	//long double size = SOME_LARGE_VALUE; //initialize to avoid reading random values.
	long double size = LDBL_MAX; //initialize to avoid reading random values.

	
	//std::cout << "Returning the stats for " << subQKey << std::endl;

	iter = resultSizeStatsTable.find(subQKey);
	
	if(iter != resultSizeStatsTable.end())
	{
		if(iter->second == 0) //what if size is 0 though?
		{ return -1; }

	//	debug_print(debug_level, Q_PLAN_LOG,"RESOLVED RS OF KEY: %s = %lu\n", subQKey, iter->second);
	
		return iter->second; 
	}

	else
	{ 
		/* gather the statistic for the this subQ */
	//	debug_print(debug_level, Q_PLAN_LOG,"COULD NOT RESOLVE RS OF KEY: %s\n", subQKey);	
	
		if(Q == NULL)
			{createSubQStr(joinNode);}
		else
			{combineSubQStrs(Q, starterNode, joinNode);}	//aisha
		
//		printf("subQ string:\n\t %s\n", joinNode->subQ->qstring);
		
		size = getSubQResultSize(joinNode->subQ->qstring, subQKey); //AISHA

		/*start insert code to keep a copy of rdf result file - AISHA
		char command[100];
		char resultfile[1024];

		//rename temp file to keep a copy for debugging purposes		
		sprintf(resultfile, "%d-%s", query_ndx, subQKey);		
		sprintf(command, "mv temp_rdf_result.txt \"%s.txt\"", resultfile);
	
		if(system(command) < 0)
				printf("Failed to rename temp_rdf_result file\n");			
		/*end insert code*/
		
		/** size of file AISHA - might want to change this to double**/ 
		//printf("\tsize of file %hd-%s.txt = %Lf\n",
		//query_ndx, subQKey, size);
	
		FILE *stream = fopen(STAT_FILE_1, "a");
		fprintf(stream, "%s\t\t\t%Lf\n", subQKey, size);
		fclose(stream);

		resultSizeStatsTable.insert(std::make_pair(subQKey,size));
	
		return size;
	}
}
	
			
/** Scan time will always be there - it's calculated in getSubQResultSize **/
long double getScanSize (Node *joinNode, char *subQKey, Query *Q, Node *starterNode)
{
	//MAP::const_iterator iter;
	MAPDOUBLE::const_iterator iter;
	
	long double size;

	//std::cout << "Returning the stats for " << subQKey << std::endl;

	iter = scanSizeStatsTable.find(subQKey);

	if(iter != scanSizeStatsTable.end())
	{
		
		//debug_print(debug_level, Q_PLAN_LOG,"RESOLVED SCAN COST OF KEY: %s = %lu\n", subQKey, iter->second);
		
		return iter->second;
	}
	
}

/** Executes sub-query. Calculates run time and stores in table.
Stores fileSize in table **/
long double getSubQResultSize(char *query, char *subQKey)
{
	long double size = 0;
	char command[100];
	char *statsfile;
	char *outfile;
	char *infile;
	FILE *stream;
	
	struct timeval start_time;
	struct timeval end_time;
	//struct timeval diff_time;

	infile    = "temp_query.txt";
	outfile   = "temp_rdf_result.txt";
	statsfile = "temp_res_size.txt";

	stream = fopen(infile, "w");
	fprintf(stream, "%s", query);
	fclose(stream);

	sprintf(command, "./rdf3xquery %s < %s > %s",
	DB, infile, outfile);

	gettimeofday(&start_time, NULL);
	if(system(command) < 0)
		printf("Failed to execute RDF3xquery");
	gettimeofday(&end_time, NULL);
	
	/** calculate runtime **/
	long double runtime = (end_time.tv_usec + 1000000 * end_time.tv_sec) - (start_time.tv_usec + 1000000 * start_time.tv_sec);
	runtime = runtime/1000000;
	//diff_time.tv_sec = difftime / 1000000;
	//diff_time.tv_usec = difftime % 1000000;

	//printf("%s runtime: %ld.%06ld seconds\n", subQKey, 	runtime);
	//printf("%s runtime: %Lf seconds\n", subQKey, runtime);
	
		
	/** save runtime in file to load in future runs **/
	stream = fopen(STAT_FILE_2, "a");
	fprintf(stream, "%s\t\t\t%Lf\n", subQKey, runtime);
	fclose(stream);

	/** save runtime in table **/	
	scanSizeStatsTable.insert(std::make_pair(subQKey,runtime));
		
		
	//sprintf(command, "wc -l %s > %s", outfile, statsfile);

	// if(system(command) < 0)
		// printf("Failed to find the result size");

	// FILE *file = fopen(statsfile, "r");
	// fscanf(file, "%lu", &size);

	if(!(stream = fopen(outfile, "r")))
		printf("Could not open temp rdf result file\n");

	fseek(stream, 0L, SEEK_END);
	size = (long double)ftell(stream);
	size = (sizeof(int8_t) * size);
	fclose(stream);

	sprintf(command, "rm %s", outfile);
	if(system(command) < 0)
		printf("Failed to remove temp_rdf_result.txt");
	
	return size;
}


/** END RUN TIME FUNCTIONS **/


/** START OLD FUNCTIONS 
ulong getResultSize (Node *joinNode, char *subQKey, Query *Q, Node *starterNode)
{
	MAP::const_iterator iter;
	
	ulong size = SOME_LARGE_VALUE; //initialize to avoid reading random values.

	iter = resultSizeStatsTable.find(subQKey);
	
	if(iter != resultSizeStatsTable.end())
	{
		if(iter->second == 0) //what if size is 0 though?
		{ return -1; }

	//	debug_print(debug_level, Q_PLAN_LOG,"RESOLVED RS OF KEY: %s = %lu\n", subQKey, iter->second);
	
		return iter->second; 
	}

	else
	{ 
		
	//	debug_print(debug_level, Q_PLAN_LOG,"COULD NOT RESOLVE RS OF KEY: %s\n", subQKey);	
	
		if(Q == NULL)
			{createSubQStr(joinNode);}
		else
			{combineSubQStrs(Q, starterNode, joinNode);}	//aisha
				
		size = getSubQResultSize(joinNode->subQ->qstring); //AISHA
		
		
		// start insert code to keep a copy of rdf result file - AISHA
		// char command[100];
		// char resultfile[1024];

		// rename temp file to keep a copy for debugging purposes		
		// sprintf(resultfile, "%d-%s", query_ndx, subQKey);		
		// sprintf(command, "mv temp_rdf_result.txt \"%s.txt\"", resultfile);
	
		// if(system(command) < 0)
				// printf("Failed to rename temp_rdf_result file\n");			
		// end insert code
		
		printf("\tsize of file %hd-%s.txt = %lu\n",
		query_ndx, subQKey, size);
	
		FILE *stream = fopen(STAT_FILE_1, "a");
		fprintf(stream, "%s\t\t\t%lu\n", subQKey, size);
		fclose(stream);

		resultSizeStatsTable.insert(std::make_pair(subQKey,size));
	
		return size;
	}
}

ulong getScanSize (Node *joinNode, char *subQKey, Query *Q, Node *starterNode)
{
	MAP::const_iterator iter;
	ulong size;

	//std::cout << "Returning the stats for " << subQKey << std::endl;

	iter = scanSizeStatsTable.find(subQKey);

	if(iter != scanSizeStatsTable.end())
	{
		//if(iter->second == 0) 
		//    { return -1; }
		
		//debug_print(debug_level, Q_PLAN_LOG,"RESOLVED SCAN COST OF KEY: %s = %lu\n", subQKey, iter->second);
		
		return iter->second;
	}
	
	else
	{
		 // gather the statistic for the this subQ
	//	debug_print(debug_level, Q_PLAN_LOG,"COULD NOT RESOLVE SCAN COST OF KEY: %s\n", subQKey);
		
		if(Q == NULL)
			{createSubQStr(joinNode);}
		else
			{combineSubQStrs(Q, starterNode, joinNode);}	
		
		size = getSubQScanSize(joinNode->subQ->qstring);

		//if(size == 0)  { return -1; }

		FILE *stream = fopen(STAT_FILE_2, "a");
		fprintf(stream, "%s\t\t\t%lu\n", subQKey, size);
		fclose(stream);

//		printf("scan cost returned by getSubQScanSize: %lu\n", size);
		scanSizeStatsTable.insert(std::make_pair(subQKey,size));

		// /*start insert code to keep a copy of rdf result file - AISHA
		// char command[100];
		// char resultfile[1024];

		// rename temp file to keep a copy for debugging purposes		
		// sprintf(resultfile, "%d-%s-scan", query_ndx, subQKey);		
		// sprintf(command, "mv temp_scan_result.txt \"%s.txt\"", resultfile);
	
		// if(system(command) < 0)
				// printf("Failed to rename temp_scan_result file\n");			
		// /*end insert code 
		return size;
	}

}

unsigned long getSubQScanSize(char *query)
{
	char command[100];
	double size = 0;
	double value;
	char *infile;
	char *outfile;
	FILE *stream;
	char *plan;

	unsigned long cost = 0;
	char *statsfile;
	
	// /*statsfile = "costFile.txt";
	
	// FILE *file = fopen(statsfile, "r");
	// fscanf(file, "%lu", &cost);

	// system("rm costFile.txt");

	// if(system(command) < 0)
	// printf("Failed to remove costFile.txt");
	// /*
	
	infile  = "temp_scan_query.txt";
	outfile = "temp_scan_result.txt";

	stream = fopen(infile, "w");
	fprintf(stream, "explain %s", query);
	fclose(stream);

	sprintf(command, "./rdf3xquery %s < %s > %s",
	DB, infile, outfile);

	if(system(command) < 0)
		printf("Failed to execute RDF3xquery");

	plan = read_file(outfile);	
	plan = strtok(plan, " ");

	while (plan != NULL)
	{
		plan = strtok (NULL, " ");

		if(plan != NULL)
			{value = strtod(plan, NULL);}
		
		if(value >= 0)
			{size += value;}
	}
	
	return (unsigned long) size;
}

unsigned long getSubQResultSize(char *query)
{
	unsigned long size = 0;
	char command[100];
	char *statsfile;
	char *outfile;
	char *infile;
	FILE *stream;

	infile    = "temp_query.txt";
	outfile   = "temp_rdf_result.txt";
	statsfile = "temp_res_size.txt";

	stream = fopen(infile, "w");
	fprintf(stream, "%s", query);
	fclose(stream);

	sprintf(command, "./rdf3xquery %s < %s > %s",
	DB, infile, outfile);

	
	if(system(command) < 0)
		printf("Failed to execute RDF3xquery");
	
	
	//sprintf(command, "wc -l %s > %s", outfile, statsfile);

	// if(system(command) < 0)
		// printf("Failed to find the result size");

	// FILE *file = fopen(statsfile, "r");
	// fscanf(file, "%lu", &size);

	if(!(stream = fopen(outfile, "r")))
		printf("Could not open temp rdf result file\n");

	fseek(stream, 0L, SEEK_END);
	size = ftell(stream);
	size = (sizeof(int8_t) * size);
	fclose(stream);

	sprintf(command, "rm %s", outfile);
	if(system(command) < 0)
		printf("Failed to remove temp_rdf_result.txt");
	
	return size;
}
/** END OLD FUNCTIONS **/

void generateSubQueryKey (Query *Q, Node *node, char **buf)
{
	int numBytes;
	CCond *set;
	int offset;

	numBytes = sprintf(*buf, "<%d>", Q->id);
	*buf += numBytes;

	for(short cid = 1; cid <= Q->num_conditions; cid++)
	{
		for(set = node->subQ->optimalset; set != NULL; set = set->next)
		{
			if(set->condition_p->id != cid)
			{ continue; }

			numBytes = sprintf(*buf, ".%hd", cid);
			*buf += numBytes;
		}		
	}

	for(Neighbor *ngbr = node->merged_ngbrs;
	ngbr != NULL; ngbr = ngbr->next)
	{
		debug_print(debug_level, Q_PLAN_LOG,"Generating the hashkey of %s's ngbr: %s\n",
			node->label, ngbr->node->label);
		generateSubQueryKey(Q, ngbr->node, buf);
	}
	
	sprintf(*buf, '\0');
}

char *combineSubQKeys (Node *node, Node *ngbr)
{
	char *suffix, *combo_hashkey;
	const char *prefix = "<";
	char token[15];

	short len = strlen(node->subQ->hashkey);

	combo_hashkey = (char*) malloc(sizeof(char) * (len+1));
	strcpy(combo_hashkey, node->subQ->hashkey);

	short offset = len;

	suffix = strtok(ngbr->subQ->hashkey, prefix);

	while (suffix != NULL)
	{
		strcpy(token, prefix);
		strcpy(token + strlen(prefix), suffix);
		int token_len = strlen(token);

		if(strstr(node->subQ->hashkey, token) == NULL)
		{
			if(!(combo_hashkey = (char*) realloc (
			combo_hashkey, sizeof(char) * (offset + token_len + 1))))
			systemError((char*)"Could not realloc in combineSubQKeys");

			strcpy(combo_hashkey + offset, token);
			offset += token_len;
		}

		suffix = strtok(NULL, prefix);
	}

	combo_hashkey[offset] = '\0';

	return combo_hashkey;
}

void combineSubQStrs (Query *Q, Node *node, Node *ngbr)
{
	char *existing_results[20];
	Condition *existing_conds[20];

	char rubbish[5];
	Condition *c;
	int numBytes;
	char *ptr;
	char *eptr;
	short cid;
	int size;
	int i;
	
	ptr = ngbr->subQ->hashkey;
	eptr = ptr + strlen(ptr);
	
	size = sizeof(existing_results);
	memset(&existing_results[0], 0, size);
	
	size = sizeof(existing_conds);
	memset(&existing_conds[0], 0, size);
	
	while(ptr < eptr)
	{
		numBytes = sscanf(ptr, "%[<]", rubbish);
        ptr += numBytes;
		
        numBytes = sscanf(ptr, "%[0-9]", rubbish);
		ptr += numBytes;     
			  
        numBytes = sscanf(ptr, "%[>]", rubbish);
        ptr += numBytes;
                
		numBytes = sscanf(ptr, ".%d", &cid);
        ptr += numBytes + 1;
		
		/* find this condition among the list of all conds. */
		
		for(c = Q->conditions; c != NULL; c = c->next)
		{
			if (c->id == cid) break;
		}
		
		/* if the condition doesn't already exist, add it */
		
		for(i = 0; existing_conds[i] != 0; i++)
		{
			if (existing_conds[i]->id == cid) continue;
		}

		existing_conds[i] = c;
	
		/* if the cond's results do not exist, add them */

		char *result = c->subj;
		
		if(result[0] == '?')
		resultExists(existing_results,result);
	
		result = c->obj;
		
		if(result[0] == '?')
		resultExists(existing_results,result);
	}

	/* form the query string: select ? ? from { ? . ? } */
	
	char *q = ngbr->subQ->qstring;
	memset(q, 0, 2000);	 // 2000 may be replaced with sizeof(q)

	sprintf(q, "select ");
	q += 7;
	
	for(i = 0; existing_results[i] != 0; i++)
	{
		numBytes = sprintf(q, "%s ", existing_results[i]);
		q += numBytes;
	}
	
	numBytes = sprintf(q, " where {");
	q += numBytes;
	
	for(i = 0; existing_conds[i] != 0; i++)
	{
		char *s = existing_conds[i]->subj;
		char *p = existing_conds[i]->pred;
		char *o = existing_conds[i]->obj;

		numBytes = sprintf(q, "%s %s %s", s, p, o);
		q += numBytes;

		if(existing_conds[i+1] != NULL)
		{
			numBytes = sprintf(q, " . ");
			q += numBytes;
		}
	}
	
	numBytes = sprintf(q, "}");
	q += numBytes;

	sprintf(q, "\0");
}

void loadResultSizeStats (const char *filename)
{
	char *subQkey;
	long double resultSize;
	FILE *stream;

	if(!(stream = fopen(filename, "r")))
		systemError((char*)"Could not load stats file\n");

	while (fscanf(stream, "%ms\t\t\t%Lf", &subQkey, &resultSize) == 2)
	{
		resultSizeStatsTable.insert(std::make_pair(subQkey,resultSize));
	}

	fclose(stream);
}

void loadScanSizeStats (const char *filename)
{
	char *subQkey;
	long double scanSize;
	FILE *stream;

	if(!(stream = fopen(filename, "r")))
		systemError((char*)"Could not load stats file\n");

	while (fscanf(stream, "%ms\t\t\t%Lf", &subQkey, &scanSize) == 2)
	{
		scanSizeStatsTable.insert(std::make_pair(subQkey,scanSize));
	}

	fclose(stream);
} 

int hasJoinNeighbor (Node *node)
{
	Neighbor *ngbr;
	
	for(ngbr = node->in_neighbors; 
	ngbr != NULL; ngbr = ngbr->next)
	{
		if(ngbr->node->isJoinNode)
		{
			return 1;
		}
	}
	
	return 0;
}

void printResultSizeStatsTable ()
{
	MAPDOUBLE::const_iterator iter;

	for(iter = resultSizeStatsTable.begin();
	iter != resultSizeStatsTable.end(); iter++)
	{
		std::cout << "element (" << iter->first <<
		"," << iter->second << ")" << std::endl;
	}
}

void printScanSizeStatsTable ()
{
	MAPDOUBLE::const_iterator iter;

	for(iter = scanSizeStatsTable.begin();
	iter != scanSizeStatsTable.end(); iter++)
	{
		std::cout << "element (" << iter->first << 
		"," << iter->second << ")" << std::endl;
	}
}
