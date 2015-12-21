#include "QueryPlanner.h"

/**
 * The QueryPlanner evaluates an optimal plan via creating a graph. The graph 
 * gives the number of join nodes and hence the number of machines that will
 * be used for that query.
 */
 
extern int debug_level;

short QID = 1;

void initQueryPlanner ()
{	
	//loadStats();
}

Query *planQuery (char *query)
{	
	Query *Q = createNewQuery();
	
	extractResults(Q, &query);
	printResults(Q);
	
	extractConditions(Q, &query);
	printConditions(Q);

	createGraph(Q);
	//printGraph(Q->graph);

	if(Q->num_join_nodes <= 1)
	{
		return Q;
	}

	assignConditionsToNodes(Q);
	
	//assignCondsToNodes_Q2(Q); //already commented out
	//assignCondsToNodes_Q9(Q);	//already commented out
	//assignCondsToNodes_newQ9(Q);	//already commented out
	//assignCondsToNodes_CQ(Q);	//already commented out

	printExclusives(Q);
	printShareds(Q);
	//printSupersets(Q);
	
	getOptimalPlan(&Q); 


	//printf("QUERYPLANNER: got optimal plan\n");

	selectWorkers(Q);
	//printf("QUERYPLANNER: got selected workers\n");

	//printSubQueries(Q);
	//printf("QUERYPLANNER: printed subqueries\n");
	
	return Q;	
}

//	void executeSubQuery(char *subQ, int w_rank)
void *executeSubQuery(void *arg)
{
	// printf("reached execute sub-query\n");
	SubQInfo info;
	char infile[20];
	char outfile[20];
	char timefile[30];
	char command[100];
	FILE *stream;

	info = *((SubQInfo *)arg);
	int w_rank = info.w_rank;
	char *subQ = info.subQ;

	sprintf(infile,  "subquery_%d.txt", w_rank);
	sprintf(outfile, "result_%d.txt",   w_rank);
	sprintf(timefile, "internal_time_%d.txt", w_rank);

	// printf("infile: %s, outfile: %s, timefile: %s\n", infile, outfile, timefile);
	
	if(!(stream = fopen(infile, "w+"))) {
		systemError("Could not open subQ file");
		debug_print(debug_level, ERROR_LOG, "%s\n", "Could not open subQ file");
	}

	fprintf(stream, "%s", subQ);
	fclose(stream);

	sprintf(command, "./rdf3xquery %s < %s > %s", 
					DB, infile, outfile);
	
	time_t startTime = time(NULL);
	
	if(system(command) < 0) {
		debug_print(debug_level, ERROR_LOG, "%s\n", "Failed to execute RDF3xquery");
	}

	time_t endTime = time(NULL);

	if(!(stream = fopen(timefile, "w+"))) {
		systemError("Could not open time file");
		debug_print(debug_level, ERROR_LOG, "%s\n", "Could not open time file");
	}
	
	int diff = endTime  - startTime;
	
	fprintf(stream, "%d", diff);
	fclose(stream);

	
	// debug_print(debug_level, Q_PLAN_LOG, "Worker %d finished resolving its sub-query in %d seconds\n", 
		// w_rank, endTime - startTime);	
}

Query *createNewQuery()
{
	Query *Q = (Query *)malloc(sizeof(Query));
	Q->id               = QID++;
	Q->results          = NULL;
	Q->conditions       = NULL;
	Q->filter           = NULL;
	Q->graph            = NULL;
	Q->optimalPlan      = NULL;
	Q->compactGraph     = NULL;
	Q->num_nodes	    = 0;
	Q->num_results      = 0;
	Q->num_conditions   = 0;
	Q->num_join_nodes   = 0;
	Q->num_merged_nodes = 0;
	return Q;	
}

SubQuery *createNewSubQuery()
{
	SubQuery *subQ = (SubQuery *)malloc(sizeof(SubQuery));
	subQ->results        = NULL;
	subQ->superset       = NULL;
	subQ->shareds        = NULL;
	subQ->exclusives     = NULL;
	subQ->optimalset     = NULL;
	subQ->conditions     = NULL;
	subQ->filter         = NULL;
	subQ->next           = NULL;
	subQ->hashkey        = NULL;
	subQ->num_results    = 0;
	subQ->num_shareds    = 0;
	subQ->num_exclusives = 0;
	subQ->num_conditions = 0;
	subQ->resultSize     = 0;
	subQ->scanSize       = 0;
	subQ->hashJoinCost   = 0;
	subQ->commCost       = 0;
	subQ->scanCost       = 0;
	return subQ;	
}

Node *createNewNode(char *label)
{
	Node *n = (Node *)malloc(sizeof(Node));
	n->label              = label;	
	n->nextRecipientNgbr  = NULL;
	n->in_neighbors       = NULL;
	n->out_neighbors      = NULL;
	n->merged_ngbrs       = NULL;
	n->next               = NULL;
	n->subQ               = NULL;	
	n->nid 				  = 0; 
	n->in_degree          = 0;
	n->out_degree         = 0;
	n->isJoinNode         = 0;
	n->is_deleted         = 0;
	n->worker_rank        = 0;

	return n;
}

Neighbor *createNewNeighbor(Node *node)
{
	Neighbor *neighbor = (Neighbor *)malloc(sizeof(Neighbor));
	neighbor->isMerged = 0;
	neighbor->node = node;
	neighbor->next = NULL;
	return neighbor;
}

void createGraph(Query *Q)
{
	Condition *c;
	Node *n, *m;

	for(c = Q->conditions; c != NULL; c = c->next)
	{
		if((n = findNode(Q->graph,c->subj)) == NULL)	
		{
			n = createNewNode(c->subj);
			addNode(&(Q->graph), &n);
			n->nid = ++(Q->num_nodes);
		}
		
		if((m = findNode(Q->graph,c->obj)) == NULL)	
		{
			m = createNewNode(c->obj);
			addNode(&(Q->graph), &m);			
			m->nid = ++(Q->num_nodes);
		}

		Neighbor *out_neighbor = createNewNeighbor(m);
		addNeighbor(&(n->out_neighbors), &out_neighbor);
		(n->out_degree)++;

		Neighbor *in_neighbor = createNewNeighbor(n);
		addNeighbor(&(m->in_neighbors), &in_neighbor);
		(m->in_degree)++;
	}

	identifyJoinNodes(Q);
}


void identifyJoinNodes(Query *Q)
{
	for(Node *n = Q->graph; n != NULL; n = n->next)
	{
		if(degree(n) > 1)
		{
			n->isJoinNode = 1;
			Q->num_join_nodes++;
		}
	}

//aisha 	debug_print(debug_level, Q_PLAN_LOG, "Total number of nodes : %hd\n", Q->num_nodes);	
	//aisha debug_print(debug_level, Q_PLAN_LOG, "Number of join nodes  : %hd\n", Q->num_join_nodes);			
}


void assignConditionsToNodes(Query *Q)
{
	short ndx = 1;	
	short eXc = 1;

	for(Condition *c = Q->conditions; c != NULL; c = c->next)
	{
		c->id = ndx++;

		Node *n = findNode(Q->graph, c->subj);
		Node *m = findNode(Q->graph, c->obj);

		if(n == NULL || m == NULL)
		{
			debug_print(debug_level, ERROR_LOG, "%s\n", "Something went wrong!!");
			return;
		}
		//if n is the join node, assign exclusive condition to it
		if(n->isJoinNode && !m->isJoinNode)  
		{
			c->isExclusive = eXc;
			assignCondition(&n,c,eXc);
		}
		//if m is the join node, assign exclusive condition to it
		else if(!n->isJoinNode &&  m->isJoinNode)
		{
			c->isExclusive = eXc;
			assignCondition(&m,c,eXc);
		}
		//if both are join nodes, assign shared condition to both
		else if( n->isJoinNode &&  m->isJoinNode)
		{
			c->isExclusive = !eXc;
			assignCondition(&n,c,!eXc);
			assignCondition(&m,c,!eXc);
		}

		/* should never occur */
		/*else 
			{ 
				Node *node = flip() ? n : m;
				c->isExclusive = eXc;
				assignCondition(&node,c,eXc);				
			}*/
	}

}

void assignCondition(Node **node, Condition *c, short isExclusive)
{
	ChosenCondition *cc;
	ChosenResult *cr;	
	Node *n = *node;

	if(n->subQ == NULL)
	n->subQ = createNewSubQuery();

	if((c->subj)[0] == '?')
	{
		if(addResult_p(&(n->subQ->results), c->subj))
		(n->subQ->num_results)++;			
	}

	if((c->pred)[0] == '?')
	{
		if(addResult_p(&(n->subQ->results), c->pred))
		(n->subQ->num_results)++;
	}

	if((c->obj)[0] == '?')
	{
		if(addResult_p(&(n->subQ->results), c->obj))
		(n->subQ->num_results)++;
	}

	
	if(isExclusive)
	{
		addCondition_p(&(n->subQ->exclusives), c);
		(n->subQ->num_exclusives)++;
	}

	else
	{
		addCondition_p(&(n->subQ->shareds), c);
		(n->subQ->num_shareds)++;
	}
	
	//addCondition_p(&(n->subQ->conditions), c);
	addCondition_p(&(n->subQ->superset), c);
	(n->subQ->num_conditions)++;
}


void assignCondsToNodes_Q2 (Query *Q)
{
	short ndx = 1;
	Node *n, *m, *p;

	n = findNode(Q->graph, "?X");

	if(n->subQ == NULL)	
	n->subQ = createNewSubQuery();

	m = findNode(Q->graph, "?Y");

	if(m->subQ == NULL)
	m->subQ = createNewSubQuery();

	p = findNode(Q->graph, "?Z");

	if(p->subQ == NULL)
	p->subQ = createNewSubQuery();
	
	for(Condition *c = Q->conditions; c != NULL; c = c->next)
	{
		c->id = ndx++;

		switch (c->id)
		{
		case 1:
			addCondition_p(&(n->subQ->optimalset), c);
			break;

		case 2:
			addCondition_p(&(m->subQ->optimalset), c);
			break;

		case 3:
			addCondition_p(&(p->subQ->optimalset), c);
			break;

		case 4:
			addCondition_p(&(p->subQ->optimalset), c);
			break;

		case 5:
			addCondition_p(&(m->subQ->optimalset), c);
			break;

		case 6:
			addCondition_p(&(n->subQ->optimalset), c);
			break;
		}
	}
}

void assignCondsToNodes_Q9 (Query *Q)
{
	short ndx = 1;
	Node *n, *m, *p;

	n = findNode(Q->graph, "?Y");

	if(n->subQ == NULL)
	n->subQ = createNewSubQuery();

	m = findNode(Q->graph, "?Z");

	if(m->subQ == NULL)
	m->subQ = createNewSubQuery();

	p = findNode(Q->graph, "?X");
	p->is_deleted = 1;

	for(Condition *c = Q->conditions; c != NULL; c = c->next)
	{
		c->id = ndx++;

		switch (c->id)
		{
		case 1:
			addCondition_p(&(n->subQ->optimalset), c);
			break;

		case 2:
			addCondition_p(&(n->subQ->optimalset), c);
			addCondition_p(&(m->subQ->optimalset), c);
			break;

		case 3:
			addCondition_p(&(m->subQ->optimalset), c);
			break;

		case 4:
			addCondition_p(&(n->subQ->optimalset), c);
			addCondition_p(&(m->subQ->optimalset), c);
			break;

		case 5:
			addCondition_p(&(m->subQ->optimalset), c);
			break;
			
		case 6:
			addCondition_p(&(n->subQ->optimalset), c);
			break;

			
		}
	}
}

void  assignCondsToNodes_newQ9(Query *Q)
{
	short ndx = 1;
	Node *n, *m, *p;

	n = findNode(Q->graph, "?Y");

	if(n->subQ == NULL)
	n->subQ = createNewSubQuery();

	m = findNode(Q->graph, "?Z");

	if(m->subQ == NULL)
	m->subQ = createNewSubQuery();

	p = findNode(Q->graph, "?X");
	p->is_deleted = 1;

	for(Condition *c = Q->conditions; c != NULL; c = c->next)
	{
		c->id = ndx++;

		switch (c->id)
		{
		case 1:
			addCondition_p(&(m->subQ->optimalset), c);
			break;

		case 2:
			addCondition_p(&(m->subQ->optimalset), c);
			break;

		case 3:
			addCondition_p(&(n->subQ->optimalset), c);
			break;

		case 4:
			addCondition_p(&(m->subQ->optimalset), c);
			break;

		case 5:
			addCondition_p(&(n->subQ->optimalset), c);
			break;

		case 6:
			addCondition_p(&(m->subQ->optimalset), c);
			break;

		}
	}

}

void assignCondsToNodes_CQ(Query *Q)
{
	short ndx = 1;
	Node *n, *m, *p, *q, *r, *s;

	n = findNode(Q->graph, "?P");

	if(n->subQ == NULL)
	n->subQ = createNewSubQuery();

	m = findNode(Q->graph, "?C");

	if(m->subQ == NULL)
	m->subQ = createNewSubQuery();

	p = findNode(Q->graph, "?D");
	p->is_deleted = 1;

	q = findNode(Q->graph, "?Y");
	q->is_deleted = 1;

	r = findNode(Q->graph, "?S");
	r->is_deleted = 1;

	s = findNode(Q->graph, "?N");
	s->is_deleted = 1;

	for(Condition *c = Q->conditions; c != NULL; c = c->next)
	{
		c->id = ndx++;

		switch (c->id)
		{
		case 1:
			addCondition_p(&(n->subQ->optimalset), c);
			break;

		case 2:
			addCondition_p(&(n->subQ->optimalset), c);
			addCondition_p(&(m->subQ->optimalset), c);
			break;

		case 3:
			addCondition_p(&(n->subQ->optimalset), c);
			addCondition_p(&(m->subQ->optimalset), c);
			break;

		case 4:
			addCondition_p(&(m->subQ->optimalset), c);
			break;

		case 5:
			addCondition_p(&(n->subQ->optimalset), c);
			break;

		case 6:
			addCondition_p(&(n->subQ->optimalset), c);
			break;

		case 7:
			addCondition_p(&(n->subQ->optimalset), c);
			break;

		case 8:
			addCondition_p(&(n->subQ->optimalset), c);
			addCondition_p(&(m->subQ->optimalset), c);
			break;

		case 9:
			addCondition_p(&(m->subQ->optimalset), c);
			break;

		case 10:
			addCondition_p(&(m->subQ->optimalset), c);
			break;

		}
	}

}

void selectWorkers(Query *Q)
{
	short status[NUM_WORKERS];
	ChosenCondition *cc;
	ChosenResult *cr;
	Node *node;
	
	for(int i = 0; i < NUM_WORKERS; i++)
	{
		status[i] = AVAILABLE;
	}

	printf("\n\nNum join nodes: %d\n\n", Q->num_join_nodes);
	
	/*traverse the optimal compact graph*/
	if(Q->compactGraph != NULL)
	{
		for(int i = 0; i < Q->num_join_nodes; i++)
		{
		
			node = Q->compactGraph[i];

			if(node->isJoinNode && !node->is_deleted
					&& node->subQ->optimalset != NULL)
			{
				assignNodeToWorker(node, status);
			}
		}
	}

	/*should be removed; it is no longer true*/
	/*traverse the original un-optimized graph*/

	else
	{
		
		//printf(	"QUERYPLANNER: selectworkers: there is no compact graph\n");
		node = Q->graph;
		
		for(node = Q->graph; node != NULL; node = node->next)
		{
			if(node->subQ != NULL)
			assignNodeToWorker(node, status);
		}
	}
}

void assignNodeToWorker(Node *node, short status[])
{
	int worker;
	srand(time(NULL));
	
	do
	{
		worker = (rand() % NUM_WORKERS);
	}

	while (status[worker] == !AVAILABLE);
	
	/* workers for query 9 (this is good
		for debugging purposes). It ensures
		that specific workers are always 
				chosen to execute a sub-query */  

	/* if(strcmp(node->label,"?Y") == 0)
		worker = 2;
	
		else if(strcmp(node->label,"?Z") == 0)
		worker = 1;

		*/

	/* workers for complex query 

		if(strcmp(node->label,"?P") == 0)
				worker = 2;

				else if(strcmp(node->label,"?C") == 0)
		worker = 1;

		*/

	/* workers for query 2 

		if(strcmp(node->label,"?X") == 0)
				worker = 2;

				else if(strcmp(node->label,"?Y") == 0)
				worker = 1;

		if(strcmp(node->label,"?Z") == 0)
				worker = 0;
		
		*/

	node->worker_rank = MIN_RANK + worker;
	// debug_print(debug_level, Q_PLAN_LOG, "Worker %hd\n", node->worker_rank);	
	status[worker]  = !AVAILABLE;
	printf(	"QUERYPLANNER: assignNode %s to worker %hd\n", node->label, node->worker_rank); //aisha


	createSubQStr(node);
	debug_print(debug_level, Q_PLAN_LOG, "subQ: %s\n\n", node->subQ->qstring);	
}


void createSubQStr(Node *node)
{
	char *existing_results[20];
	short num_results = 0;
	int numBytes;
	char *q;
	int n;
	
	int size = sizeof(existing_results);
	memset(&existing_results[0], 0, size);

	q = node->subQ->qstring;
	memset(q, 0, 2000);      // 2000 may be replaced with sizeof(q)

	n  = sprintf(q, "select ");
	q += n;

		
	createResultsStr(node, existing_results, &num_results, &q, 0);
	node->subQ->num_results = num_results;

	n  = sprintf(q, " where {");
	q += n;
	
	createConditionsStr(node, &q);
	
	n  = sprintf(q, "}");
	q += n;

	sprintf(q, "\0");
}


void createResultsStr(Node *node, char *existing_results[], short *num_results, char **buf, int level)
{
	ChosenCondition *set;
	char *result;
	int n;

	if(node->subQ->optimalset != NULL)
	{
		set = node->subQ->optimalset;
	}

	else
	{
		set = node->subQ->superset;
	}

	for(; set != NULL; set = set->next)
	{
		result = set->condition_p->subj;			
		
		if(result[0] == '?' &&
		!resultExists(existing_results,result))
		{
			(*num_results)++;		
		}

		result = set->condition_p->obj;
		
		if(result[0] == '?' && 
		!resultExists(existing_results,result))
		{
			(*num_results)++;
		}

	}

	Neighbor *ngbr = node->merged_ngbrs;

	for(; ngbr != NULL; ngbr = ngbr->next)
	{
		/* you may want to add a check that if the
		ngbr->node->is_deleted { continue } */
		createResultsStr(ngbr->node,
		existing_results, num_results, buf, level++);
	}
	
	if(level == 0)
	{
		bubbleSort(existing_results,*num_results);

		for(int i = 0; i < *num_results; i++)
		{
			char *result = existing_results[i];
			n = sprintf(*buf, "%s ", result);
			*buf += n;
		}

		sprintf(*buf, "\0");
	}

}


void createConditionsStr(Node *node, char **buf)
{
	ChosenCondition *set;
	int n;

	if(node->subQ->optimalset != NULL)
	{
		set = node->subQ->optimalset;
	}

	else
	{
		set = node->subQ->superset;
	}

	for(; set != NULL; set = set->next)
	{
		char *s = set->condition_p->subj;
		char *p = set->condition_p->pred;
		char *o = set->condition_p->obj;

		n = sprintf(*buf, "%s %s %s", s, p, o);
		*buf += n;

		if(set->next != NULL)
		{
			n = sprintf(*buf, " . ");
			*buf += n;
		}
	}

	Neighbor *ngbr = node->merged_ngbrs;
	for(; ngbr != NULL; ngbr = ngbr->next)
	{
		n = sprintf(*buf, " . ");
		*buf += n;

		createConditionsStr(ngbr->node, buf);
	}

	sprintf(*buf, "\0");	
}


int resultExists(char *existing_results[], char *new_result)
{
	int i;

	for(i = 0; existing_results[i] != 0; i++)
	{
		if(!strcmp(existing_results[i],new_result))
		return 1;
	}

	existing_results[i] = new_result;
	
	return 0;
}

void bubbleSort (char *elems[], int num_elems)
{
	int sorted;

	do
	{
		sorted = 1;

		for(int i = 1; i <= num_elems-1; i++)
		{
			if(strcmp(elems[i-1],elems[i]) > 0)
			{
				char *temp = elems[i-1];
				elems[i-1] = elems[i];
				elems[i]   = temp;

				sorted = 0;
			}
		}

	} while (!sorted);
}

void createSubQueryStr(Node *n)
{
	ChosenResult *cr;
	ChosenCondition *cc;
	char *q;
	int nb;

	q  = n->subQ->qstring;                
	nb  = sprintf(q, "select ");
	q += nb;

	
	for(cr = n->subQ->results; cr != NULL; cr = cr->next)
	{
		nb = sprintf(q, "%s", cr->variable);
		q += nb;

		if(cr->next != NULL)
		{
			nb = sprintf(q, " ");
			q += nb;
		}

		else
		{
			nb = sprintf(q, " where {");
			q += nb;
		}

	}

	if(n->subQ->optimalset != NULL)	
	{
		debug_print(debug_level, Q_PLAN_LOG, "%s\n", "OPTIMAL SET IS NOT NULL");	
		cc = n->subQ->optimalset;
	}

	else
	{
		debug_print(debug_level, Q_PLAN_LOG, "%s\n", "SUPERSET SET IS NOT NULL");	
		cc = n->subQ->superset;
	}

	for(; cc != NULL; cc = cc->next)
	{
		char *s = cc->condition_p->subj;
		char *p = cc->condition_p->pred;
		char *o = cc->condition_p->obj;

		nb = sprintf(q, "%s %s %s", s, p, o);
		q += nb;

		if(cc->next != NULL)
		{
			nb = sprintf(q, " . ");
			q += nb;
		}
		
		else
		{
			nb = sprintf(q, "}");
			q += nb;
		}
	}

	sprintf(q, "\0");
}

/*************************************FUNCTIONS TO PARSE SPARQL QUERY*********************************************************/


void extractResults(Query *Q, char **query)
{
	char *token = NULL;

	token = extractToken(query);

	if(strcmp(token,"select"))
	{
		parseError("Missing a select clause");
	}

	token = extractToken(query);

	while (strcmp(token,"where")) 
	{
		Result *result   = (Result *) malloc(sizeof(Result));
		result->variable = token;
		result->exists   = 0;
		result->next     = NULL;			
		
		addResult(&Q->results, &result);
		(Q->num_results)++;
		
		token = extractToken(query);
	} 				
}

void  extractConditions(Query *Q, char **query)
{
	char *token = NULL;

	if((*query)[0] != '{')
	{
		parseError(" Missing opening brace { in where clause");
		debug_print(debug_level, ERROR_LOG, "%s\n", "Missing opening brace { in where clause");
	}
	

	if(!strrchr(*query,'}'))
	{	
		parseError("Missing closing brace } in where clause");
		debug_print(debug_level, ERROR_LOG, "%s\n", "Missing closing brace } in where clause");
	}

	do
	{			
		*query += 1;
		extractCondition(Q,query);
		
	} while((*query = strchr(*query,'.')) != NULL);
}

void extractCondition(Query *Q, char **query)
{
	char *token;
	int string_len = 0;
	
	Condition *condition = (Condition *) malloc(sizeof(Condition));

	condition->subj = extractToken(query);
	condition->pred = extractToken(query);
	condition->obj  = extractToken(query);				
	condition->id   = ++(Q->num_conditions);
	condition->next = NULL;
	
	addCondition(&Q->conditions, &condition);
}


char *extractToken(char **query)
{
	char *token, *trimmed_token;
	int offset;

	if(*query == '\0') return NULL;

	sscanf(*query, " %ms %n", &token, &offset);
	sscanf(token, "%m[^}]",  &trimmed_token);
	(*query) += offset;
	free(token);

	return trimmed_token;
}

/*************************************UTILITY LIST FUNCTIONS************************************************************/

int flip()
{
	return (rand() % 2);
}

short degree(Node *n)
{
	return n->in_degree + n->out_degree;
}

Node *findNode(Node *nodes, char *label)
{
	Node *n;

	for(n = nodes; n != NULL; n = n->next)
	if(strcmp(n->label,label) == 0)
	return n;
	
	return NULL;
}

Neighbor *findNeighbor(Neighbor *ngbrs, char *label)
{
	Neighbor *ngbr;

	for(ngbr = ngbrs; ngbr != NULL; ngbr = ngbr->next)
	if(strcmp(ngbr->node->label,label) == 0)
	return ngbr;

	return NULL;
}


Neighbor *removeNeighbor(Neighbor **neighbors, char *label)
{
	Neighbor *ngbr, *prevNgbr = NULL;
	
	if(*neighbors == NULL) return NULL;

	for(ngbr = *neighbors; ngbr!= NULL; ngbr = ngbr->next)
	{
		if(!strcmp(ngbr->node->label,label))
		break;
		
		prevNgbr = ngbr;
	}

	/* ngbr was not found */
	if(!ngbr) { return NULL; }

	if(prevNgbr)
	{
		/*remove ngbr from middle*/
		prevNgbr->next = ngbr->next;
	}

	else
	{
		/*remove ngbr from head*/
		*neighbors = ngbr->next;
	}

	return ngbr;
}

ChosenResult *findResult(ChosenResult *results, char *variable)
{
	ChosenResult *cr;

	for(cr = results; cr != NULL; cr = cr->next)
	if(strcmp(cr->variable, variable) == 0)
	return cr;

	return NULL;		
}

/*************************************FUNCTIONS TO ADD LIST ITEMS************************************************************/


void addResult(Result **list, Result **element)
{
	Result *r;

	if(*list == NULL)
	{
		*list = *element;
		return;
	}

	for(r = *list; r->next != NULL; r = r->next);		
	r->next = *element; 
}

int addResult_p(ChosenResult **list, char *str)
{
	ChosenResult *element;

	if(findResult(*list, str) != NULL)  return 0;

	element = (ChosenResult *) malloc(sizeof(ChosenResult));
	element->variable = str;
	
	for(; *list != NULL; list = &(*list)->next)
	if(strcmp((*list)->variable, element->variable) > 0)
	break;

	element->next = *list;
	*list = element;
	
	return 1;
}

void addChosenResult(ChosenResult **list, ChosenResult **element)
{
	for(; *list != NULL; list = &(*list)->next)
	if(strcmp((*list)->result_p->variable, (*element)->result_p->variable) > 0)
	break;

	(*element)->next = *list;
	*list = (*element);
}

void addCondition(Condition **list, Condition **element)
{
	Condition *c;

	if(*list == NULL)
	{
		*list = *element;
		return;
	}

	for(c = *list; c->next != NULL; c = c->next);
	c->next = *element;
}

void addCondition_p(ChosenCondition **list, Condition *cond)
{
	ChosenCondition *element;

	if(!(element = (ChosenCondition *) malloc(sizeof(ChosenCondition))))
	systemError("Couldn't malloc a new chosen condition");

	element->condition_p = cond;
	
	element->next = *list;
	*list = element;
}

void addChosenCondition(ChosenCondition **list, ChosenCondition **element)
{
	(*element)->next = *list;
	*list = *element;
}

void addNode(Node **nodes, Node **node)
{
	(*node)->next = *nodes;
	*nodes = *node;
}

void addNeighbor(Neighbor **neighbors, Neighbor **neighbor)
{
	Neighbor *n;
	
	if(*neighbors == NULL)
	{
		*neighbors = *neighbor;
		return;
	}
	
	for(n = *neighbors; n->next != NULL; n = n->next);
	n->next = *neighbor;
	
	//(*neighbor)->next = *neighbors;
	//*neighbors = *neighbor;
}

void addSubQuery(SubQuery **subQueries, SubQuery **subQ)
{
	(*subQueries)->next = *subQ;
	*subQ = *subQueries;
}

/*************************************PRINT FUNCTIONS*****************************************************************/

void printQuery(Query *Q)
{
	Result *r;
	Condition *c;


	debug_print(debug_level, Q_PLAN_LOG, "%s", "SPARQL QUERY:\n\n");		
	debug_print(debug_level, Q_PLAN_LOG, "%s",  "select ");		
	
	for(r = Q->results; r != NULL; r = r->next)
	{
		debug_print(debug_level, Q_PLAN_LOG, "%s", r->variable);		
		
		if(r->next != NULL) {
			debug_print(debug_level, Q_PLAN_LOG, "%s",  ", ");		
		}
			
		else {
			debug_print(debug_level, Q_PLAN_LOG, "%s",  " where {");					
		}
	}

	for(c = Q->conditions; c != NULL; c = c->next)
	{
		debug_print(debug_level, Q_PLAN_LOG, "%s %s %s", c->subj, c->pred, c->obj);		
		
		if(r->next != NULL) {
			debug_print(debug_level, Q_PLAN_LOG, "%s",  " . ");				
		}
		else {
			debug_print(debug_level, Q_PLAN_LOG, "%s",  "}\n\n");				
		}
	}
}	

//aisha - re-enable debugs
void printSubQueries(Query *Q)
{
	Node *n;

	debug_print(debug_level, Q_PLAN_LOG, "%s",  "SUB QUERIES :\n");				
//	printf("QUERYPLANNER: SUB QUERIES :\n");				

	for(n = Q->graph; n != NULL; n = n->next)
	{
		printf("Node (%s)\n", n->label);
		if(n->subQ != NULL && !n->is_deleted)
			debug_print(debug_level, Q_PLAN_LOG, "%s\n\n", n->subQ->qstring);
//			printf("%s\n\n", n->subQ->qstring);							
	}
}

void printSubQuery(SubQuery *subQ)
{
	ChosenResult *r;
	ChosenCondition *c;

	if(subQ == NULL)
	return;

	debug_print(debug_level, Q_PLAN_LOG, "%s",  "select ");	
	for(r = subQ->results; r != NULL; r = r->next)
	{
		debug_print(debug_level, Q_PLAN_LOG, "%s", r->variable);			
		if(r->next != NULL) {
			debug_print(debug_level, Q_PLAN_LOG, "%s",  ", ");			
		}
		else {
			debug_print(debug_level, Q_PLAN_LOG, "%s",  " where {");			
		}
	}

	for(c = subQ->conditions; c != NULL; c = c->next)
	{
		char *s = c->condition_p->subj;
		char *p = c->condition_p->pred;
		char *o = c->condition_p->obj;

		debug_print(debug_level, Q_PLAN_LOG, "%s %s %s", s, p, o);			
		
		if(c->next != NULL) {
			debug_print(debug_level, Q_PLAN_LOG, "%s",  " . ");
		}
		
		else {
			debug_print(debug_level, Q_PLAN_LOG, "%s",  "}\n\n");
		}
		
	}
}

void printResults(Query *Q)
{
	Result *r;

	debug_print(debug_level, Q_PLAN_LOG, "KEYS[%hd] : ", Q->num_results);			

	for(r = Q->results; r != NULL; r = r->next) {
		debug_print(debug_level, Q_PLAN_LOG, "%s ", r->variable);			
	}

	debug_print(debug_level, Q_PLAN_LOG, "%s",  "\n\n");			
}

void printConditions(Query *Q)
{
	Condition *c;

	debug_print(debug_level, Q_PLAN_LOG, "CONDITIONS[%hd] :\n", Q->num_conditions);			

	for(c = Q->conditions; c != NULL; c = c->next) {
		debug_print(debug_level, Q_PLAN_LOG, "(%x,%hd,%s,%s,%s)\n", c, c->id, c->subj, c->pred, c->obj);			
	}

	debug_print(debug_level, Q_PLAN_LOG, "%s",  "\n");			
}


//aisha - restore all debugs
void printShareds(Query *Q)
{	
	ChosenCondition *cc;

	debug_print(debug_level, Q_PLAN_LOG, "%s",  "Shareds: \n");			
//	printf("QUERY PLANNER: Shareds: \n");			

	for(Node *n = Q->graph; n != NULL; n = n->next)
	{
		if(n->subQ != NULL)
		{
			debug_print(debug_level, Q_PLAN_LOG, "Shareds of node (%s):\n", n->label);			
//			printf("\tShareds of node (%s):\n", n->label);			

			for(cc = n->subQ->shareds; cc != NULL; cc = cc->next)
				debug_print(debug_level, Q_PLAN_LOG, "(%x, %hd %s,%s,%s)\n", cc->condition_p,
					cc->condition_p->id, cc->condition_p->subj, 
					cc->condition_p->pred, cc->condition_p->obj);			
//				printf("\t(%x, %hd %s,%s,%s)\n", cc->condition_p,

		}
		debug_print(debug_level, Q_PLAN_LOG, "%s",  "\n");
//		printf("\n");			
	}
}


//aisha - restore all debugs
void printExclusives(Query *Q)
{	
	ChosenCondition *cc;

	debug_print(debug_level, Q_PLAN_LOG, "%s",  "Exclusives: \n");						

	for(Node *n = Q->graph; n != NULL; n = n->next)
	{
		if(n->subQ != NULL)
		{
			debug_print(debug_level, Q_PLAN_LOG, "Exclusives of node (%s):\n", n->label);						

			for(cc = n->subQ->exclusives; cc != NULL; cc = cc->next)
				debug_print(debug_level, Q_PLAN_LOG, "(%x, %hd %s,%s,%s)\n", cc->condition_p,
					cc->condition_p->id, cc->condition_p->subj, 
					cc->condition_p->pred, cc->condition_p->obj);			
//				printf("\t(%x, %hd %s,%s,%s)\n", cc->condition_p,

		}
		debug_print(debug_level, Q_PLAN_LOG, "%s",  "\n");
//		printf("\n");			
	}
}


//aisha - restore all debugs
void printSupersets(Query *Q)
{	
	ChosenCondition *cc;

	debug_print(debug_level, Q_PLAN_LOG, "%s",  "Supersets: \n");			
//	printf("QUERY PLANNER: Supersets: \n");			

	for(Node *n = Q->graph; n != NULL; n = n->next)
	{
		if(n->subQ != NULL)
		{
			debug_print(debug_level, Q_PLAN_LOG, "Superset of node (%s):\n", n->label);			
//			printf("\tSuperset of node (%s):\n", n->label);			

			for(cc = n->subQ->superset; cc != NULL; cc = cc->next)
			
			debug_print(debug_level, Q_PLAN_LOG, "(%x, %hd %s,%s,%s)\n", cc->condition_p, 
				cc->condition_p->id, cc->condition_p->subj, 
				cc->condition_p->pred, cc->condition_p->obj);			
//			printf("\t(%x, %hd %s,%s,%s)\n", cc->condition_p, 

		}
		debug_print(debug_level, Q_PLAN_LOG, "%s",  "\n");			
//		printf("\n");
	}
}
 
void printGraph(Node *nodes)
{
	Node *n;
	
	debug_print(debug_level, Q_PLAN_LOG, "%s",  "GRAPH :\n");			

	debug_print(debug_level, Q_PLAN_LOG, "%s",  "");			

	for(n = nodes; n != NULL; n = n->next)
	{	

		debug_print(debug_level, Q_PLAN_LOG, "%s",  "Node : ");			
		debug_print(debug_level, Q_PLAN_LOG, "(label:%s) ", n->label);			
		char isJoin = (n->isJoinNode) ? 'Y' : 'N';
		debug_print(debug_level, Q_PLAN_LOG, "(isJoinNode:%c) ", isJoin);			
		debug_print(debug_level, Q_PLAN_LOG, "%s",  "(inNeighbors: ");			
		printNeighbors(n->in_neighbors);
		debug_print(debug_level, Q_PLAN_LOG, "%s",  ") ");			
		debug_print(debug_level, Q_PLAN_LOG, "%s", "(outNeighbors: ");			
		printNeighbors(n->out_neighbors);
		debug_print(debug_level, Q_PLAN_LOG, "%s",  ") ");			
		debug_print(debug_level, Q_PLAN_LOG, "%s",  "\n");			
	}

	debug_print(debug_level, Q_PLAN_LOG, "%s",  "\n");			
}

void printNeighbors(Neighbor *neighbors)
{
	Neighbor *n;
	
	debug_print(debug_level, Q_PLAN_LOG, "%s",  "{");			

	for(n = neighbors; n != NULL; n = n->next)
	{
		debug_print(debug_level, Q_PLAN_LOG, "(label:%s,", n->node->label);			
		char isJoin = (n->node->isJoinNode) ? 'Y' : 'N';
		debug_print(debug_level, Q_PLAN_LOG, "isJoinNode:%c)", isJoin);			

		if(n->next != NULL) {
		debug_print(debug_level, Q_PLAN_LOG, "%s",  ",");						
		}
	}

	debug_print(debug_level, Q_PLAN_LOG, "%s",  "}");			
	
}

char *read_file(char *filename)
{
	FILE *fd;
	long fsize;
	char *query;

	if((fd = fopen(filename, "r")) == NULL)
	printf("Could not open query file");

	fseek(fd, 0, SEEK_END);
	fsize = ftell(fd);
	fseek(fd, 0, SEEK_SET);

	if(!(query = malloc(sizeof(char) * (fsize + 1))))
	printf("Could not allocate buffer in readQuery");

	fread(query, sizeof(char), fsize, fd);
	fclose(fd);

	query[fsize] = '\0';

	return query;
}