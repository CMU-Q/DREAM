#include "Optimizer.h"

CCond ***allSets;

CCond **optimalSets;
long double optimalCost;
Node **optimalCompactGraph;

short *positions;
short *numSets;

int numSlots;
int numPlans = 0;
int numMergedPlans = 0;

extern "C" int debug_level;

long double networkSpeed = 20000000.0;

void calculateNetworkSpeed(){
	char speed_test_script[20] = "speed-test.sh";
	char resultfile[20] = "speed.txt";
	char other_machine[20] = "aishah-n2";
	char command[100];
	int fsize;
	FILE *stream;
	
	//run speed test script to get upload and download speed		
	sprintf(command, "./%s %s %s\n", speed_test_script, other_machine, resultfile);
	
	if(system(command) < 0)
		printf("Failed to run speed test script\n");			
	
	//get upload speed from script
	if(!(stream = fopen(resultfile, "r")))
		printf("Could not open network speed result file\n");
	printf("opened speed.txt\n");
	fscanf(stream, "Upload %Lf kB/s\n", &networkSpeed);
	fclose(stream);
	printf("closed speed.txt\n");
	
	printf("networkSpeed: %Lf kB/s\n", networkSpeed);
	networkSpeed *= KILO;
	printf("networkSpeed: %Lf B/s\n", networkSpeed);
}


void getOptimalPlanHelper (Query **Q)
{
	initOptimizer(*Q);
	if(SHOULD_GET_NETWORK_SPEED){
		calculateNetworkSpeed();
	}
	createCompactGraph_v2(Q);
	printCompactGraph((*Q)->compactGraph, (*Q)->num_join_nodes); //aisha
	//aisha debug_print(debug_level, Q_PLAN_LOG, "%s\n", "Created compact graph");
	
	enumerateAllSets(*Q); //all possible subgraphs that can be assigned to each Join Vertex
	//aisha debug_print(debug_level, Q_PLAN_LOG, "%s\n", "Enumerated all sets");
	//aisha printAllEnumeratedSets(*Q);//aisha	

	//calculateNetworkSpeed();
	findOptimalCompactGraph(*Q); //gets cost of all possible sub-graph combinations 
	
	debug_print(debug_level, Q_PLAN_LOG,"%s\n", "------------OPTIMAL BASE GRAPH-----------------");
	//printCompactGraph((*Q)->compactGraph, (*Q)->num_join_nodes); //aisha
	debug_print(debug_level, Q_PLAN_LOG,"Plan's cost: %lu\n", optimalCost);
	//aisha debug_print(debug_level, Q_PLAN_LOG,"%s\n", "------------------------------------------------");

	updateCompactGraph(Q);	//update optimalCompactGraph and Q->compactGraph with optimalSets

/*aisha	print stored optimalSets. (compare with compactGraph)
	printf("OPTIMIZER: optimal set of subGraphs\n");
	for(int k = 0; k < numSlots; k++){
		printf("\tJV %d\n", k);
		CCond* temp;
		
		for(temp = optimalSets[k]; temp != NULL; temp = temp->next){
			Condition *cond = temp->condition_p;
			printf("\toptimal SG: %s ---> %s ---> %s\n", cond->subj, cond->pred, cond->obj);
		}
		
		printf("\n");
	}
*/
		
/*aisha	print compactgraph
	printf("OPTIMIZER: compactGraph\n");
	
	for(int k = 0; k< (*Q)->num_join_nodes; k++){
		printf("\tJV %s\n", (*Q)->compactGraph[k]->label);
		
		CCond* temp = (*Q)->compactGraph[k]->subQ->optimalset;
		
		for(temp; temp != NULL; temp = temp->next){
			Condition *cond = temp->condition_p;
			printf("\toptimal SG: %s ---> %s ---> %s\n", cond->subj, cond->pred, cond->obj);
		}
		
		printf("\n");
	}
*/

//	debug_print(debug_level, Q_PLAN_LOG,"%s\n", "------------OPTIMAL BASE GRAPH-----------------");
//	debug_print(debug_level, Q_PLAN_LOG,"%s\n", "The optimal base plan is:");
//	printCompactGraph((*Q)->compactGraph, (*Q)->num_join_nodes); //aisha - print compactgraph if you need to.
//	debug_print(debug_level, Q_PLAN_LOG,"Plan's cost: %lu\n", optimalCost);
//	debug_print(debug_level, Q_PLAN_LOG,"%s\n", "------------------------------------------------");
	

//	debug_print(debug_level, Q_PLAN_LOG,"%s\n", "Started compacting");

	/*AISHA UNCOMMENT*/
	mergeCompactGraph(*Q);	
	updateCompactGraph(Q);
	setOptimalPlan(Q);
	
	debug_print(debug_level, Q_PLAN_LOG,"%s\n", "------------OPTIMAL MERGED GRAPH-----------------");
	debug_print(debug_level, Q_PLAN_LOG,"Plan's cost (optimal): %Lf\n", optimalCost);
	
//	debug_print(debug_level, Q_PLAN_LOG,"%s\n", "Finished compacting");

	//aisha printf("OPTIMIZER: CompactGraph after merge, after updating compactGraph\n");
	//aisha printCompactGraph((*Q)->compactGraph, (*Q)->num_join_nodes);
	
	//aisha printf("OPTIMIZER: optimalCompactGraph after merge, after updating compactGraph\n");
	//aisha printCompactGraph(optimalCompactGraph, (*Q)->num_join_nodes);	
}

void initOptimizer (Query *Q)
{
	numSlots 	 = Q->num_join_nodes;

	if(!(allSets     = (CCond***) malloc(sizeof(CCond**) * numSlots))
			|| !(optimalSets = (CCond**)  malloc(sizeof(CCond*)  * numSlots))
			|| !(numSets     = (short*)   malloc(sizeof(short)   * numSlots))
			|| !(positions   = (short*)   malloc(sizeof(short)   * numSlots)))
	{
		systemError((char*)"Optimizer could not malloc its structures");
		debug_print(debug_level, ERROR_LOG,"%s\n", "Optimizer could not malloc its structures");
	}

	for(int slot = 0; slot < numSlots; slot++)
	{
		positions[slot]   = 0;
		optimalSets[slot] = NULL;
	}

	//optimalCost = SOME_LARGE_VALUE;
	optimalCost = LDBL_MAX;
	optimalCompactGraph = NULL;
}

void createCompactGraph (Query **Q)
{
	printf("in create compact graph\n");
	int ndx = 0;

	if(!((*Q)->compactGraph = (Node **) malloc(sizeof(Node *) * (*Q)->num_join_nodes)))
	{
		systemError((char*)"Optimizer could not create temp compact Graph");
		debug_print(debug_level, ERROR_LOG, "%s\n", "Optimizer could not create temp compact Graph");
	}

	for(Node *n = (*Q)->graph; n != NULL; n = n->next)
	{
		if(n->isJoinNode)
		{
			n->nextRecipientNgbr = n->in_neighbors;
			((*Q)->compactGraph)[ndx++] = n;
		}
	}	

	optimalCompactGraph = (*Q)->compactGraph;	
}

void printAllEnumeratedSets(Query *Q){
	printf("OPTIMIZER: printAllSets\n");
	Node *n;
	int slot = 0;

	for(n = Q->graph; n != NULL; n = n->next)
	{
		if(n->isJoinNode)
		{
			printSets(n, slot++);
		}
	}
}

void enumerateAllSets (Query *Q)
{
	Node *n;
	int slot = 0;

	for(n = Q->graph; n != NULL; n = n->next)
	{
		if(n->isJoinNode)
		{
			enumerateSets(n, slot++);
		}
	}
}

/* print possible sets for a given Join Vertex*/
void printSets (Node *n, short slot)
{
	int num_sets, set_size;

	numSets[slot] = 1 << n->subQ->num_shareds;
	num_sets = numSets[slot];

	printf("\tprinting sets for Node (%s), %d possible sets of SG\n",
				n->label, numSets[slot]);


	for (int i = 0; i < numSets[slot]; i++){
		printf("\tallSets[slot][i] = allSets[%d][%d]\n", slot, i);
	
		for(CCond *cc = allSets[slot][i]; cc != NULL; cc = cc->next)
		{
			Condition *temp = cc->condition_p;
			printf("SG: %s ---> %s ---> %s\n", temp->subj, temp->pred, temp->obj);
		}

	}
	
	printf("\n");
}

/* enumerate possible sets for a given Join Vertex*/
void enumerateSets (Node *n, short slot)
{
	int num_sets, set_size;

	numSets[slot] = 1 << n->subQ->num_shareds;
	num_sets = numSets[slot];

	//create a list of sets 
	if(!(allSets[slot] = (CCond **) malloc(sizeof(CCond*) * num_sets)))
	{
		systemError((char*)"Could not create a list of sets at a slot");
		debug_print(debug_level, ERROR_LOG, "%s\n", "Could not create a list of sets at a slot");
	}
	
	//smallest set contains only exclusive subgraphs of given Join Vertex 
	allSets[slot][0] = n->subQ->exclusives;	

	//largest set contains all subgraphs of given Join Vertex 	
	allSets[slot][num_sets-1] = n->subQ->superset;

	num_sets -= 2;	
	set_size  = n->subQ->num_exclusives + 1;

	for(int i = 1; i < num_sets; i += (1 << i)) //some funky arithmetic!
	{
		CCond *cc = n->subQ->shareds;

		//add subgraphs one by one to previous list
		for(int j = 0; cc != NULL; cc = cc->next, j++) 
		{
			createSet(set_size++, &allSets[slot][i+j],
				allSets[slot][i-1], cc);
		}
	}
}


/* union list and elem into one set */
void createSet (int size, CCond **new_set, CCond *list, CCond *elem)
{	
	CCond *ptr;
	int i;

	if(!(*new_set = (CCond *) malloc(sizeof(CCond) * size))) {
		systemError((char*)"Could not malloc a new set");
		debug_print(debug_level, ERROR_LOG, "%s\n", "Could not malloc a new set");
	}
	
	ptr = *new_set;
	i = 0;
	
	for(CCond *cc = list; cc != NULL; cc = cc->next, i++)
	{
		ptr[i].condition_p = cc->condition_p;
		ptr[i].next = &(ptr[i+1]);
	}

	ptr[i].condition_p = elem->condition_p;
	ptr[i].next = NULL;
}

/* a plan is a combination of sets, one for each join vertex. 
Each plan is stored in the temp compact graph */
void findOptimalCompactGraph (Query *Q)
{
	do
	{
		if(isValidPlan(Q))
		{
			numPlans++;
				
			/*Aisha - will execute Plan 2
			int tempPlanNo = 1;
			if(numPlans == tempPlanNo){
				printf("----------------> PLAN %d\n", numPlans);
				estimatePlanCost(Q);
				break;
			}
			*/
				
			
			/* AISHA - change this back*/
			printf("----------------> PLAN %d\n", numPlans);
			estimatePlanCost(Q);
			
			
			//temp(Q);
		}
		
	} while (getNextPlan());
	
	
	//printf("plan %d is optimalPlan\n", numPlans);
	

}


/*
* Updates positions[] to tell estimatePlanCost which set of subGraphs
* to select for the JV = "slot".
*
* Positions[] is updated in an iterative manner:
* 000 001 002...010 011 012...020 021 022..100 101 102...110 etc.
*/
int getNextPlan ()
{
	int slot;

	for(slot = 0; slot < numSlots; slot++)
	{
		positions[slot]++;

		if(positions[slot] < numSets[slot])
		{
			return 1;
		}
		
		/*reset index to 0*/
		positions[slot] = 0;
	}
	
	return 0;
}

/* this function was used for testing purposes */
void temp (Query *Q)
{
	ulong cost;

	if(numPlans == DESIRED_PLAN_NDX)
	{
		for(int i = 0; i < numSlots; i++)
		{
			(Q->compactGraph)[i]->subQ->optimalset
			= allSets[i][positions[i]];
		}

		//cost = estimateTotalPlanCost(Q,Q->compactGraph);
		//printCompactGraph(Q->compactGraph, Q->num_join_nodes);
		// printf("Cost: %lu\n", cost);
		debug_print(debug_level, Q_PLAN_LOG,"Cost: %lu\n", cost);
		optimalCost = cost;
		
		for(int i = 0; i < numSlots; i++)
		{
			optimalSets[i] = allSets[i][positions[i]];
		}
	}

}

/*
* Estimates cost for a valid plan (see getNextPlan).
* It stores the valid plan in compactGraph's optimalSet.
* The cost is then compared against the cost stored in optimalCost.
* If the new cost is lower, then optimalSet stores the valid plan.
*/
void estimatePlanCost (Query *Q)
{
	long double cost = 0;
			
	/* store the next plan (composed of series of sets)
	in the compact graph (the input expected by the
	cost estimator)                              */

	for(int i = 0; i < numSlots; i++)
	{
		(Q->compactGraph)[i]->subQ->optimalset 
		= allSets[i][positions[i]];
	}

	cost = estimateTotalPlanCost(Q,Q->compactGraph);
	/*AISHA UNCOMMENT*/
	printCompactGraph(Q->compactGraph, Q->num_join_nodes); //aisha - cost of graph plans
	
	debug_print(debug_level, Q_PLAN_LOG,"PLAN'S COST: %Lf\n", cost);
	
	if((long double) cost < 0)
	{
		debug_print(debug_level, Q_PLAN_LOG,"%s", "REJECTED THE ABOVE COMPACT GRAPH");
		debug_print(debug_level, Q_PLAN_LOG,"%s\n", "AS IT PRODUCES INTERNAL ERROR:");
		return; 
	}

	if(cost < optimalCost)
	{
		optimalCost = cost;
		
		/* remember the plan i.e. the series of sets */
		for(int i = 0; i < numSlots; i++)
		{
			optimalSets[i] = allSets[i][positions[i]];
		}
	}
}

void rememberPlan (ulong planCost)
{
	optimalCost = planCost;
	
	for(int i = 0; i < numSlots; i++)
	optimalSets[i] = allSets[i][positions[i]];
}

/* excludes plans where all sub-graphs are not included*/
int isValidPlan (Query *Q)
{
	CCond *set, *cc;
	short numQueries;
	short id;
	int mask;

	int isAssigned = 0;

	for(int slot = 0; slot < numSlots; slot++)
	{
		set = allSets[slot][positions[slot]];         
		numQueries = 0;         

		for(cc = set; cc != NULL; cc = cc->next)
		{
			id           = cc->condition_p->id;
			mask 	     = (1 << (id-1));
			isAssigned  |= mask;
			numQueries++;
		}

		if(numQueries <= 1) { return 0; } //quicly excludes case where 1 sub-graph assigned to a join node
	}

	int expectedOutput = 1 << Q->num_conditions;
	return (isAssigned+1 == expectedOutput ? 1 : 0);	
}

void updateCompactGraph (Query **Q)
{
	for(int i = 0; i < (*Q)->num_join_nodes; i++)
	{
		Node *node = ((*Q)->compactGraph)[i];
		node->subQ->optimalset = optimalSets[i];
	}
	
	(*Q)->compactGraph = optimalCompactGraph;
	//optimalCompactGraph = (*Q)->compactGraph;	AISHA - this line was changed
}

void mergeCompactGraph (Query *Q)
{
	debug_print(debug_level, Q_PLAN_LOG, "%s\n", "inside mergeCompactGraph");
	mergeCompactGraphRecuresively (Q, Q->compactGraph, 0, 0);
}

void mergeCompactGraphRecuresively (Query *Q, Node *compactGraph[], int level, int ID)
{
	int ndxMerge, ndxRecv; /* indices of the merge and recipient nodes resp. */
	long double cost;	   /* plan cost */
	++level;
	
	for(ndxMerge = 0; ndxMerge < Q->num_join_nodes; ndxMerge++)
	{
		Node *mergeNode = compactGraph[ndxMerge];
	
	debug_print(debug_level, Q_PLAN_LOG, "ndxMerge: %d, Node %s\n",
	ndxMerge, mergeNode->label);
		
		/* if there are no more in-ngbrs to merge
		this node with, move to the next node */
		if(!mergeNode->nextRecipientNgbr)
		{
			debug_print(debug_level, Q_PLAN_LOG,
			"level %d: Node %s has no in-ngbrs\n",
			level,mergeNode->label);
			continue;
		}
		
		while(mergeNode->nextRecipientNgbr != NULL)
		{
			/* if the next recipient out-ngbr is deleted
				then advance to the next in-ngbr */
			
			if(mergeNode->nextRecipientNgbr->node->is_deleted)
			{
				debug_print(debug_level, Q_PLAN_LOG, "level %d: Node %s is deleted\n",
				level, mergeNode->nextRecipientNgbr->node->label);
		
				mergeNode->nextRecipientNgbr = 
				mergeNode->nextRecipientNgbr->next;
				
				debug_print(debug_level, Q_PLAN_LOG, "level %d: Moving on to Node %s\n",
				level, mergeNode->nextRecipientNgbr->node->label);
				
				continue;
			}
			
			/* find the index of recipient node in the graph*/
			for(ndxRecv = 0; ndxRecv < Q->num_join_nodes; ndxRecv++)
				if(!strcmp(mergeNode->nextRecipientNgbr->node->label, 
				compactGraph[ndxRecv]->label))
					break;

			/* point to the next in-ngbr to merge the node with*/
			mergeNode->nextRecipientNgbr = mergeNode->nextRecipientNgbr->next;
			
			/* create a new graph in which mergeNode is merged
			with the recipientNode (the node before advancing
			the pointer) */
			Node **newCompactGraph = buildNewCompactGraph
			(compactGraph, Q->num_join_nodes, ndxRecv, ndxMerge);
			
				
			/*Aisha - will execute Plan 2*/
			
			//if(isValidCompactPlan(Q, newCompactGraph))
			//{
				
			/*numMergedPlans++;
			
			int tempMergedPlanNo = 2;
			if(numMergedPlans == tempMergedPlanNo){
				cost = estimateTotalPlanCost(Q, newCompactGraph);
				break;
			}*/
			
			numMergedPlans++;
			printf("----------------> MERGED PLAN %d\n", numMergedPlans);
			cost = estimateTotalPlanCost(Q, newCompactGraph);
			
			/*AISHA UNCOMMENT*/
			printCompactGraph(newCompactGraph, Q->num_join_nodes); //aisha
			debug_print(debug_level, Q_PLAN_LOG,"MERGED PLAN'S COST: %Lf\n", cost);

			/** AISHA check this **/
			//if((long) cost > 0 && cost < optimalCost)
				/* if the cost was actually 0 - (i.e. an empty result set
				*  for a merged node), the optimizer would pick an incorrect
				*  cost as the optimal.
				*/
			if((long)cost < optimalCost)
			{
				optimalCompactGraph = newCompactGraph;
				optimalCost = cost;
			}

			mergeCompactGraphRecuresively(Q, newCompactGraph, level, ndxMerge);
		}
	}
	
	return;
}


Node **buildNewCompactGraph (Node *compactGraph[], int numJoinNodes, 
int ndxOfRecipientNode, int ndxOfMergedNode)
{
	Neighbor *ngbr;
	Node **newCompactGraph;
	Node *recipientNode, *mergedNode;
	
	copyCompactGraph(&newCompactGraph, compactGraph,  
	numJoinNodes, ndxOfMergedNode);
	
	recipientNode = newCompactGraph[ndxOfRecipientNode];
	mergedNode    = newCompactGraph[ndxOfMergedNode]; 
	
	/* add the merged node to the recipient's list of merged neighbors */

	Neighbor *newNgbr = createNewNeighbor(mergedNode);
	addNeighbor(&(recipientNode->merged_ngbrs), &newNgbr);
	
	/* remove the merged node from the out-neighbors of recipient node */

	if(removeNeighbor(&(recipientNode->out_neighbors), mergedNode->label))
	(recipientNode->out_degree)--;	
	
	/* remove the merged node from the in-neighbors of recipient node */

	if(removeNeighbor(&(recipientNode->in_neighbors), mergedNode->label))
	(recipientNode->in_degree)--;	
	
	/* union out-neighbors of the merged node with the recipient node's */

	for(ngbr = mergedNode->out_neighbors; ngbr != NULL; ngbr = ngbr->next)
	{
		char *label = ngbr->node->label;

		/* if the recipient node itself is an out-ngbr, skip it*/
		if(!strcmp(recipientNode->label,label)) { continue; }

		/* if the node is already an in-ngbr of the recipient node, 
			skip it, as adding it would create a cycle between them */
		if(findNeighbor(recipientNode->in_neighbors, label)) { continue; }
		
		/* if the node is not already an out_ngbr add it as one */
		if(!findNeighbor(recipientNode->out_neighbors, label))
		{
			Neighbor *newNgbr = createNewNeighbor(ngbr->node);
			addNeighbor(&(recipientNode->out_neighbors),&newNgbr);
			(recipientNode->out_degree)++;
		}
	}

	/* union in-neighbors of the merged node with the recipient node's */

	for(ngbr = mergedNode->in_neighbors; ngbr != NULL; ngbr = ngbr->next)
	{
		char *label = ngbr->node->label;
		
		/* if the recipient node itself is an in-ngbr, skip it*/
		if(!strcmp(recipientNode->label,label)) { continue; }
		
		/* if the node is already an out-ngbr of the recipient node, 
			skip it, as adding it would create a cycle between them */
		if(findNeighbor(recipientNode->out_neighbors, label)) { continue; }
		
		/* if the node is not already an in_ngbr, add it as one */
		if(!findNeighbor(recipientNode->in_neighbors, label))
		{
			Neighbor *newNgbr = createNewNeighbor(ngbr->node);
			addNeighbor(&(recipientNode->in_neighbors),&newNgbr);
			(recipientNode->in_degree)++;
		}
	}
	
	/* change any pointers to the merged node to point to the recipient node */

	for(int ndx = 0; ndx < numJoinNodes; ndx++)
	{
		Node *node = newCompactGraph[ndx];

		if(ndx == ndxOfMergedNode) { continue; }
		
		/* unnecessary but for completeness */
		if(ndx == ndxOfRecipientNode) { continue; }
		
		if(ngbr = findNeighbor(node->out_neighbors, mergedNode->label))
		{
			/* if the recipient node is an already an out-ngbr, there is no
				need to add it; only remove the reference to the merged node */
			
			if(findNeighbor(node->out_neighbors, recipientNode->label))
			{
				removeNeighbor(&(node->out_neighbors), mergedNode->label);
				(node->out_degree)--;
			}
			
			/* if the recipient node is an already an in-ngbr, there is no
				need to add it as doing so would create a cycle between the node
				& recipient node; only remove the reference to the merged node */
			
			else if(findNeighbor(node->in_neighbors, recipientNode->label))
			{
				removeNeighbor(&(node->out_neighbors), mergedNode->label);
				(node->out_degree)--;
			}
			
			/* otherwise update the reference to the merged node to point to
				the recipient node; */
			
			else
			{
				ngbr->node = recipientNode;
			}
		}
	}
	
	/* change any pointers to the merged node to point to the recipient node */

	for(int ndx = 0; ndx < numJoinNodes; ndx++)
	{
		Node *node = newCompactGraph[ndx];

		if(ndx == ndxOfMergedNode) { continue; }
		
		/* unnecessary but for completeness */
		if(ndx == ndxOfRecipientNode) { continue; }

		if(ngbr = findNeighbor(node->in_neighbors, mergedNode->label))
		{
			/* if the recipient node is an already an in-ngbr, there is no
				need to add it; only remove the reference to the merged node */
			
			if(findNeighbor(node->in_neighbors, recipientNode->label))
			{
				removeNeighbor(&(node->in_neighbors), mergedNode->label);
				(node->in_degree)--;
			}

			/* if the recipient node is an already an out-ngbr, there is no
				need to add it as doing so would create a cycle between the node
				& recipient node; only remove the reference to the merged node */
			
			else if(findNeighbor(node->out_neighbors, recipientNode->label))
			{
				removeNeighbor(&(node->out_neighbors), mergedNode->label);
				(node->out_degree)--;
			}
			
			/* otherwise update the reference to the merged node to point to
				the recipient node */
			else
			{
				ngbr->node = recipientNode;
			}
		}
	}
	
	/*merge the two subQs of the merged recipient nodes

	addSubQuery(&recipientNode->subQ, &mergedNode->subQ);*/

	return newCompactGraph;
}


void copyCompactGraph (Node **newCompactGraph[], Node *oriCompactGraph[], 
int numJoinNodes, int ndxOfMergedNode)
{	
	Node *node, *copyOfNode;

	if(!(*newCompactGraph = (Node**) malloc(sizeof(Node*) * numJoinNodes)))
	{
		systemError((char*)"Could not create a new compact graph");
	}

	/* create a copy of each node */
	
	for(int i = 0; i < numJoinNodes; i++)
	{
		(*newCompactGraph)[i] = (Node*) malloc(sizeof(Node));
	}
	
	/* copy the basic information of each node to 
		the corresponding copy of the node */
	
	for(int i = 0; i < numJoinNodes; i++)
	{
		node 				= oriCompactGraph[i];
		copyOfNode 			= (*newCompactGraph)[i];

		copyOfNode->nid			= node->nid;
		copyOfNode->subQ          	= node->subQ;
		copyOfNode->label         	= node->label;
		copyOfNode->isJoinNode    	= node->isJoinNode;
		copyOfNode->is_deleted 		= (i == ndxOfMergedNode) ? 1 : node->is_deleted;
		
		copyOfNode->merged_ngbrs	= NULL;
		copyOfNode->in_neighbors  	= NULL;
		copyOfNode->out_neighbors 	= NULL;		 
		copyOfNode->nextRecipientNgbr 	= NULL;
		
		copyOfNode->in_degree     	= 0;
		copyOfNode->out_degree    	= 0;
	}

	/* deep copy the lists of in-ngbrs, out-ngbrs, merged-ngbrs
		from each node to the corresponding copy of the node */
	
	for(int i = 0; i < numJoinNodes; i++)
	{
		int j;
		
		node 		= oriCompactGraph[i];
		copyOfNode 	= (*newCompactGraph)[i];
		
		/* deep copy the out_neighbors */
		
		for(Neighbor *ngbr = node->out_neighbors; 
		ngbr != NULL; ngbr = ngbr->next)
		{
			if(!ngbr->node->isJoinNode) { continue; }

			/* find the position of this ngbr in the new graph */
			/* if(ngbr->node->nid == (*newCompactGraph)[j]->nid) */
			for(j = 0; j < numJoinNodes; j++)
			if(!strcmp(ngbr->node->label, (*newCompactGraph)[j]->label))
			break;	

			Neighbor *copyOfNgbr = createNewNeighbor((*newCompactGraph)[j]);
			addNeighbor(&(copyOfNode->out_neighbors), &copyOfNgbr);
			copyOfNode->out_degree++;
		}

		/* deep copy of the in_neigbors */
		
		for(Neighbor *ngbr = node->in_neighbors; 
		ngbr != NULL; ngbr = ngbr->next)
		{
			if(!ngbr->node->isJoinNode) { continue; }			

			/* find the position of this ngbr in the new graph */
			for(j = 0; j < numJoinNodes; j++)
			if(!strcmp(ngbr->node->label, (*newCompactGraph)[j]->label))
			break;

			Neighbor *copyOfNgbr = createNewNeighbor((*newCompactGraph)[j]);
			addNeighbor(&(copyOfNode->in_neighbors), &copyOfNgbr);
			copyOfNode->in_degree++;
		}
		
		/* deep copy the merged_neighbors */
		
		for(Neighbor *ngbr = node->merged_ngbrs;
		ngbr != NULL; ngbr = ngbr->next)
		{
			for(j = 0; j < numJoinNodes; j++)
			if(!strcmp(ngbr->node->label, (*newCompactGraph)[j]->label))
			break;
			
			Neighbor *copyOfNgbr = createNewNeighbor((*newCompactGraph)[j]);
			addNeighbor(&(copyOfNode->merged_ngbrs), &copyOfNgbr);
		} 
		
		/* set the nextRecipientNgbr to point to the head of the 
			in-ngbrs list */ 
		
		copyOfNode->nextRecipientNgbr = copyOfNode->in_neighbors;
		
		/* if the node is the merged node, set nextRecipientNgbr 
			to point to the next in-ngbr that the node is to be 
			merged node with in the subsequent merge */
		
		if(i <= ndxOfMergedNode)
		{
			if(!node->nextRecipientNgbr)
			{
				copyOfNode->nextRecipientNgbr = NULL;
			}
			
			else
			{	
				while(strcmp(copyOfNode->nextRecipientNgbr->node->label,
				node->nextRecipientNgbr->node->label))
				{
					copyOfNode->nextRecipientNgbr = 
					copyOfNode->nextRecipientNgbr->next;
				}
			}			
		}
	}
}

int isValidCompactPlan (Query *Q, Node *compactGraph[])
{
	int allJoinNodes = 0;		   /* bit representation of all nodes */
	int mergedNodes  = 0;		   /* bit representation of a node's ngbrs */
	short illegalNodeExists = 0;   /* flag to remember an illegal node in the CG*/
	short numRecipientNodes = 0;   /* counter for the number of recipient nodes */
	
	/* create the bit representation of all the join nodes */
	/* can possibly done once in createCompactGraph and 
	stored for re-usability in Q 					   */
	for(int i = 0; i < Q->num_join_nodes; i++)
	{
		Node *node 	  = compactGraph[i];
		int mask 	  = 1 << (node->nid-1);
		allJoinNodes |= mask;
	}
	
	debug_print(debug_level, Q_PLAN_LOG, "allJoinNodes: %x\n", allJoinNodes);
	
	/* create the bit representation of all the merged nodes of a node */
	for(int i = 0; i < Q->num_join_nodes; i++)
	{
		mergedNodes = 0;
		Node *node  = compactGraph[i];
		
		if(node->is_deleted) continue; else numRecipientNodes++;		
		traverseMergedNodes(node, compactGraph, &mergedNodes);
		
		debug_print(debug_level, Q_PLAN_LOG, "MergedNodes of %s:%x\n", node->label, mergedNodes+1);
		if(mergedNodes+1 == allJoinNodes)
		illegalNodeExists = 1;
	}
	
	return ((numRecipientNodes > 1 && illegalNodeExists) ? 1 : 0); 
}

void traverseMergedNodes (Node *node, Node *compactGraph[], int *mergedNodes)
{
	for(Neighbor *ngbr = node->merged_ngbrs; 
	ngbr != NULL; ngbr = ngbr->next)
	{
		int mask = 1 << (ngbr->node->nid-1);
		*mergedNodes |= mask;
		
		traverseMergedNodes (ngbr->node, compactGraph, mergedNodes);
	}
}

/* create function to find the index of a node: findNdxOfNode (Graph, label) or (Graph, node) */
/* create function CopyBasicInfo(Node *source, Node *dest); */

void createCompactGraph_v2 (Query **Q)
{
	int i = 0;

	if(!((*Q)->compactGraph = (Node **) malloc(sizeof(Node *) * (*Q)->num_join_nodes)))
	{
		systemError((char*)"Optimizer could not initialize compact Graph");
		debug_print(debug_level, ERROR_LOG,"%s\n", "TEST");
	}

	/* malloc the nodes and set the next pointer so that
		the array can be iterated as a linked list too 
		
	for(i = 0; i < (*Q)->num_join_nodes); i++)
	{
		((*Q)->compactGraph)[i] = (Node*) 
				malloc(sizeof(Node);
	} 
	
	for(i = 0; i < (*Q)->num_join_nodes); i++)
	{
		((*Q)->compactGraph)[i]->next = 
			((*Q)->compactGraph)[i+1];
	} */
	
	/* copy the basic information of each join node */
	// i = 0;
	
	for(Node *n = (*Q)->graph; n != NULL; n = n->next)
	{
		//Node *copyOfNode = ((*Q)->compactGraph)[i++];
		Node *copyOfNode = ((*Q)->compactGraph)[i];

		if(!n->isJoinNode) continue;
		
		((*Q)->compactGraph)[i]                = (Node*) malloc(sizeof(Node));
		((*Q)->compactGraph)[i]->nid		   = n->nid;
		((*Q)->compactGraph)[i]->subQ          = n->subQ;
		((*Q)->compactGraph)[i]->label         = n->label;
		((*Q)->compactGraph)[i]->isJoinNode    = n->isJoinNode;
		((*Q)->compactGraph)[i]->is_deleted    = n->is_deleted;
		((*Q)->compactGraph)[i]->merged_ngbrs  = n->merged_ngbrs;
		
		((*Q)->compactGraph)[i]->in_neighbors  = NULL;
		((*Q)->compactGraph)[i]->out_neighbors = NULL;
		((*Q)->compactGraph)[i]->nextRecipientNgbr = NULL;
		
		((*Q)->compactGraph)[i]->in_degree     = 0;
		((*Q)->compactGraph)[i]->out_degree    = 0;
		
		/* erase this index update */
		i++;
	}
	
	i = 0;
	
	for(Node *n = (*Q)->graph; n != NULL; n = n->next)
	{
		int j;
		
		if(!n->isJoinNode) { continue; }
		
		/*Node *copyOfNode = ((*Q)->compactGraph)[i++]*/
		Node *copyOfNode = ((*Q)->compactGraph)[i];
		
		/* copy the out_neighbors of each node to its new copy node */
		
		for(Neighbor *ngbr = n->out_neighbors; ngbr != NULL; ngbr = ngbr->next)
		{
			if(!ngbr->node->isJoinNode) { continue; }

			for(j = 0; j < (*Q)->num_join_nodes; j++)
			if(!strcmp(ngbr->node->label, ((*Q)->compactGraph)[j]->label))
			break;	

			Neighbor *copyOfNgbr = createNewNeighbor(((*Q)->compactGraph)[j]);
			addNeighbor(&(((*Q)->compactGraph)[i]->out_neighbors), &copyOfNgbr);
			((*Q)->compactGraph)[i]->out_degree++;
		}

		/* copy of the in_neigbors of each node to its new copy node */
		
		for(Neighbor *ngbr = n->in_neighbors; ngbr != NULL; ngbr = ngbr->next)
		{
			if(!ngbr->node->isJoinNode) { continue; }			

			for(j = 0; j < (*Q)->num_join_nodes; j++)
			if(!strcmp(ngbr->node->label, ((*Q)->compactGraph)[j]->label))
			break;

			Neighbor *copyOfNgbr = createNewNeighbor(((*Q)->compactGraph)[j]);
			addNeighbor(&(((*Q)->compactGraph)[i]->in_neighbors), &copyOfNgbr);
			((*Q)->compactGraph)[i]->in_degree++;
		}
		
		/* update the nextRecipientNgbr to point to the start of a node's 
			in-ngbrs list or to the next in-ngbr if the node is the merged node */
		
		((*Q)->compactGraph)[i]->nextRecipientNgbr =
		((*Q)->compactGraph)[i]->in_neighbors;
		
		/* erase this index update */
		i++;
		
	}
	
	optimalCompactGraph = (*Q)->compactGraph;	
}

void setOptimalPlan (Query **Q)
{
	(*Q)->compactGraph = optimalCompactGraph;
	(*Q)->estimateCost = optimalCost;

	for(int i = 0; i < (*Q)->num_join_nodes; i++)
	{
		Node *node = ((*Q)->compactGraph)[i];

		if(node->isJoinNode &&
				node->is_deleted)
		{
			((*Q)->num_merged_nodes)++;
		}
	}
}

void copyNeighbors (Neighbor **dest, Neighbor *src)
{
	Neighbor *ngbr, *copyOfNgbr;

	for(ngbr = src; ngbr != NULL; ngbr = ngbr->next)
	{
		copyOfNgbr = createNewNeighbor(ngbr->node);
		copyOfNgbr->isMerged = 0;

		addNeighbor(dest, &copyOfNgbr); 
	}
}

void printCompactGraph (Node *compactGraph[], int num_join_nodes)
{
	Node *n;

//aisha	debug_print(debug_level, Q_PLAN_LOG,"%s\n", "COMPACT GRAPH :");
	printf("OPTIMIZER: Compact graph\n");

	for(int i = 0; i < num_join_nodes; i++)
	{
		n = compactGraph[i];

		
/*aisha		debug_print(debug_level, Q_PLAN_LOG,"%s", "NODE ---> ");

		debug_print(debug_level, Q_PLAN_LOG,"(id:%hd,", n->nid);
		debug_print(debug_level, Q_PLAN_LOG,"label:%s,", n->label);
		debug_print(debug_level, Q_PLAN_LOG,"isDeleted:%hd,", n->is_deleted);
		debug_print(debug_level, Q_PLAN_LOG,"subQ hashkey: %s,", n->subQ->hashkey);
		debug_print(debug_level, Q_PLAN_LOG,"in_degree:%hd,", n->in_degree);
		debug_print(debug_level, Q_PLAN_LOG,"out_degree:%hd) ", n->out_degree); */
		
		printf("\tNODE ---> \n");

		printf("\t(id:%hd, ", n->nid);
		printf("label:%s, ", n->label);
		printf("isDeleted:%hd, ", n->is_deleted);
		printf("subQ hashkey: %s, ", n->subQ->hashkey);
		printf("in_degree:%hd, ", n->in_degree);
		printf("out_degree:%hd)\n", n->out_degree); 

/*aisha		debug_print(debug_level, Q_PLAN_LOG,"%s", "(in-ngbrs: ");
		printNgbrs(n->in_neighbors);
		debug_print(debug_level, Q_PLAN_LOG,"%s", ") ");

		debug_print(debug_level, Q_PLAN_LOG,"%s", "(out-ngbrs: ");
		printNgbrs(n->out_neighbors);
		debug_print(debug_level, Q_PLAN_LOG,"%s", ") ");

		debug_print(debug_level, Q_PLAN_LOG,"%s", "(merged-ngbrs: ");
		printNgbrs(n->merged_ngbrs);
		debug_print(debug_level, Q_PLAN_LOG,"%s", ") ");

		debug_print(debug_level, Q_PLAN_LOG,"%s", "\n");
*/

		printf("\t\tin-ngbrs: ");
		printNgbrs(n->in_neighbors);
		printf("\n");

		printf("\t\tout-ngbrs: ");
		printNgbrs(n->out_neighbors);
		printf("\n");

		printf("\t\tmerged-ngbrs: ");
		printNgbrs(n->merged_ngbrs);
		printf("\n");
	}
}

void printNgbrs (Neighbor *neighbors)
{
	Neighbor *n;

	for(n = neighbors; n != NULL; n = n->next)
	{
//aisha		debug_print(debug_level, Q_PLAN_LOG,"(label:%s,isMerged:%hd)", n->node->label, n->isMerged);
		printf("(label:%s,isMerged:%hd)", n->node->label, n->isMerged);

		if(n->next != NULL) 
		printf(", ");

//aisha		debug_print(debug_level, Q_PLAN_LOG, "%s", ", ");
	}
}

void printAllSets ()
{
	for(int i = 0; i < numSlots; i++)
	{
		debug_print(debug_level, Q_PLAN_LOG,"All Sets of Join Node %d:\n", i);

		for(int j = 0; j < numSets[i]; j++)
		{
			debug_print(debug_level, Q_PLAN_LOG,"Set %d: ", j);

			CCond *set = allSets[i][j];
			printSet(set);
			debug_print(debug_level, Q_PLAN_LOG, "%s", "\n");

		}
	}
}

void printSet (CCond *set)
{
	debug_print(debug_level, Q_PLAN_LOG, "%s", "[ ");
	
	for(CCond *ptr = set; ptr != NULL; ptr = ptr->next)
	{
		debug_print(debug_level, Q_PLAN_LOG,"%hd ", ptr->condition_p->id);
	}
	debug_print(debug_level, Q_PLAN_LOG, "%s", "] ");
	
}

void printOptimalPlan ()
{
	/*	printf("Optimal Plan: ");

	for(int i = 0; i < numSlots; i++)
	{
		printSet(optimalSets[i]);
	}

	printf("\n");
*/
}

void cleanDatastructures ()
{
	for(int i = 0; i < numSlots; i++)
	{
		/*for(int j = 0; i < numSets[i]; j++)
				{
						printf("j is %d\n", j);
						printf("freeing set %d %d\n", i, j);
						free(allSets[i][j]);
				}*/

		//printf("freeing set %d\n", i);
		free(allSets[i]);
	}

	free(numSets);
	free(positions);
}

