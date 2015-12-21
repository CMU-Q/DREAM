#include "newHashJoin.h"

OuterRelation *outer_relation;
InnerRelation *inner_relation;

pthread_mutex_t mutex;
pthread_cond_t cond;

int worker_rank;
Output *output;

void joinResultSets (int my_rank, short *my_num_keys, char **my_keys[],
         short ngbr_num_keys, char *ngbr_keys[], char *ngbr_result_set, 
	 short final_num_keys, char *final_keys[], pthread_t subQ_tid, pthread_t ngbr_tid)
{
	 printf("Worker %d in join result sets\n", my_rank);

	char timefile[30];
	char my_result_set[20];
	FILE *stream;

	worker_rank = my_rank;

        initDatastructures();

	outer_relation->isEmpty = 0;
	inner_relation->isEmpty = 0;

	outer_relation->subQ_tid = subQ_tid;
	inner_relation->ngbr_tid = ngbr_tid;

        outer_relation->num_keys = *my_num_keys;
        inner_relation->num_keys = ngbr_num_keys;
		
        sprintf(my_result_set, "result_%d.txt", my_rank);
	output->num_join_keys = countJoinKeys(*my_keys, ngbr_keys);
	output->num_cols      = *my_num_keys + ngbr_num_keys;
	output->num_cols     -= output->num_join_keys;
	output->filename      = my_result_set;

        findJoinKeys(*my_keys, ngbr_keys);
       
	mergeKeys(my_keys, ngbr_keys, 
	final_num_keys, final_keys);

	*my_num_keys = output->num_cols;

	time_t startTime = time(NULL);

	pthread_cond_init(&cond, NULL);
	cond = PTHREAD_COND_INITIALIZER;

        pthread_create(&(outer_relation->tid), NULL,
             partitionOuterRelation, my_result_set);

 	pthread_create(&(inner_relation->tid), NULL,  
	   partitionInnerRelation, ngbr_result_set);

	int num_threads_done = 0;
	
	while (num_threads_done != 2)
	{
			pthread_mutex_lock(&mutex);
        	pthread_cond_wait(&cond, &mutex);
        	pthread_mutex_unlock(&mutex);

		num_threads_done++;

		if(outer_relation->isEmpty 
                || inner_relation->isEmpty)
		{
			//if(pthread_cancel(outer_relation->tid) < 0)
			//printf("ERROR killing outer thread: %s\n", strerror(errno));

			//if(pthread_cancel(inner_relation->tid) < 0)
			//printf("ERROR killing inner thread: %s\n", strerror(errno));

			//if(pthread_cancel(outer_relation->subQ_tid) < 0)
			//printf("ERROR killing subQ thread: %s\n", strerror(errno));
	
			//if(pthread_cancel(inner_relation->ngbr_tid) < 0)
                        //printf("ERROR killing ngbr thread: %s\n", strerror(errno));
                	
			finalizeJoin();
                	clean();

			printf(">>> Worker %d, DONE JOINING\n", worker_rank);
			return;
		}
	}

	//if(pthread_cancel(outer_relation->tid) < 0)
        //printf("ERROR killing outer thread: %s\n", strerror(errno));

        //if(pthread_cancel(inner_relation->tid) < 0)
        //printf("ERROR killing inner thread: %s\n", strerror(errno));

	//if(pthread_cancel(outer_relation->subQ_tid) < 0)
        //printf("ERROR killing subQ thread: %s\n", strerror(errno));

        //if(pthread_cancel(inner_relation->ngbr_tid) < 0)
        //printf("ERROR killing ngbr thread: %s\n", strerror(errno));
	

	matchRelations();
	clean();
	
	time_t endTime = time(NULL);

	printf("Worker %d, DONE JOINING in %d seconds\n", worker_rank, endTime - startTime);
}

int isEmpty (char *result_set)
{
        return !strcmp(result_set, "<empty result>\n");
}

void finalizeJoin ()
{
        FILE *stream = fopen(output->filename, "w+");
        fwrite("<empty result>\n", sizeof(char), 15, stream);
        fclose(stream);
}

short countJoinKeys (char *keys1[], char *keys2[])
{
        short count = 0;

        for(short i = 0; i < outer_relation->num_keys; i++)
        {
                for(short j = 0; j < inner_relation->num_keys; j++)
               	{
                       	if(!strcmp(keys1[i], keys2[j]))
                        {
                                count++;
                        }
                }
        }

	return count;
}


void findJoinKeys (char *keys1[], char *keys2[])
{
        short *ndxs1, *ndxs2, num_join_keys;
        short z = 0;

        num_join_keys = output->num_join_keys;

        if(!(ndxs1 = (short *) malloc(sizeof(short) * num_join_keys))
        || !(ndxs2 = (short *) malloc(sizeof(short) * num_join_keys)))
        {
                systemError((char*)"malloc failed in findJoinKeys");
        }

        for(short i = 0; i < outer_relation->num_keys; i++)
        {
               	for(short j = 0; j < inner_relation->num_keys; j++)
                {
                        if(!strcmp(keys1[i], keys2[j]))
                        {
                                ndxs1[z] = i;
                                ndxs2[z] = j;
                                z++;
                        }
                }
        }

	outer_relation->join_keys_ndxs = ndxs1;
        inner_relation->join_keys_ndxs = ndxs2;
}

void *partitionOuterRelation (void *arg)
{
	//printf("Worker %d in partition outer relation\n", worker_rank);

	long fsize;
	long dangler;
	long chunk_size;
	char *relation;
	void *status;
	FILE *stream;

	int num_threads = 4;
	Args args[num_threads];
        void *statuses[num_threads];
        pthread_t tids[num_threads];

	//time_t start_time = time(NULL);

	pthread_join(inner_relation->tid, &status);
	pthread_join(outer_relation->subQ_tid, &status);

	char *filename = (char *) arg;
	stream = fopen(filename, "r");
	fseek(stream, 0, SEEK_END);
        fsize = ftell(stream);
        fseek(stream, 0, SEEK_SET);

        if(!(relation = (char*) malloc(sizeof(char) * (fsize + 1))))
        systemError((char*)"Could not allocate buffer in readQuery");

	fread(relation, sizeof(char), fsize, stream);
        relation[fsize] = '\0';
	fclose(stream);

	if(isEmpty(relation))
	{ 
		//printf("Worker %d, inner relation is empty; sending a signal\n", worker_rank);
		pthread_mutex_lock(&mutex);
  		outer_relation->isEmpty = 1;
		pthread_cond_signal(&cond);
  		pthread_mutex_unlock(&mutex);
		return NULL;
	}

	chunk_size = fsize / num_threads;
        dangler = fsize % num_threads;

	//printf("WORKER %d, SIZE OF OUTER RELATION: %ld\n", worker_rank, fsize);
	//printf("WORKER %d, CHUNK SIZE OF OUTER RELATION: %ld\n", worker_rank, chunk_size);

	/* open the output stream 
        if(!output->stream)
        {
                output->stream = fopen(output->filename, "w+");
                fprintf(output->stream, "<empty result>\n");
                fseek(output->stream, 0L, SEEK_SET);
        }*/

	for(int i = 0; i < num_threads; i++)
        {
		char filename[25];
		
		sprintf(filename, "%s.%d", 
		  output->filename, i);

                args[i].instream   = relation;
	        args[i].chunk_size = chunk_size;
                args[i].offset     = i * chunk_size;
		args[i].outstream  = fopen(filename, "w+");

                if(i == num_threads - 1)
                {
                        args[i].chunk_size += dangler;
                        partitionOuterRelationHelper(&args[i]);
                }

                else
                {
                        if(pthread_create(&tids[i], NULL,
                        partitionOuterRelationHelper, &args[i]))
                        systemError((char*)"Could not create new thread");
                }
        }

        for(int i = 0; i < num_threads - 1; i++)
        {
                pthread_join(tids[i], &statuses[i]);
        }

	
	/* open the output stream */
        
	if(!output->stream)
        {
                output->stream = fopen(output->filename, "w+");
                fprintf(output->stream, "<empty result>\n");
                fseek(output->stream, 0L, SEEK_SET);
	}

	for(int i = 0; i < num_threads; i++)
	{
		char *result;

		fseek(args[i].outstream, 0, SEEK_END);
		fsize = ftell(args[i].outstream);
		fseek(args[i].outstream, 0, SEEK_SET);
	
		if(!(result = (char*) malloc(sizeof(char) * (fsize + 1))))
        	systemError((char*)"Could not allocate buffer in readQuery");

        	fread(result, sizeof(char), fsize, args[i].outstream);
        	result[fsize] = '\0';

		fclose(args[i].outstream);
		fwrite(result, sizeof(char), fsize, output->stream);
	}

	fclose(output->stream);

	//time_t end_time = time(NULL);
	
	//char *task = (char*) "OUTER PARTITIONING";
	//getUsage(task);

        pthread_mutex_lock(&mutex);
        pthread_cond_signal(&cond) ;
       	pthread_mutex_unlock(&mutex) ;

	//printf("WORKER %d FINISHED PARTITIONING OUTER RELATION IN %d SECONDS!!\n", worker_rank, end_time - start_time);

        return NULL;

}

void *partitionOuterRelationHelper (void *arg)
{
	//printf("Worker %d, Thread %ld in outerRelationHelper\n", worker_rank, (long)pthread_self());

        MAP::accessor a;
        char *start_ptr;
        char *end_ptr;
        ulong key;
	short i;

        Args args       = *((Args *) arg);
	char *relation  = (char *) args.instream;
        FILE *outstream = (FILE *) args.outstream;
	long chunk_size = (long) args.chunk_size;
	long offset     = (long) args.offset;

        short num_keys = outer_relation->num_keys;
        short join_key_ndx = outer_relation->join_keys_ndxs[0];

        start_ptr  = relation;
        start_ptr += offset;

	end_ptr  = start_ptr;
        end_ptr += chunk_size;

	/* adjust the end-pointer to point to the
        end of the last record in the chunk ***/

        while (*end_ptr != '\0' && *end_ptr != '\n')
        {
                end_ptr++;
        }

	/* required so that end-pointer points to 
	to an actual value rather than \n\0 ***/

        end_ptr -= 2;

	/* adjust the start-pointer to point to the
	beginning of the next complete record ***/

	if(offset != 0)
	{
		while (*start_ptr != '\0' 
		    && *start_ptr != '\n')
        	{
                	start_ptr++;
        	}

		start_ptr += 1;
	}

	long num_lines = 0;
	Record *new_record = createNewRecord(num_keys);

        while(start_ptr < end_ptr)
	{                
		num_lines++;

		for(i = 0; i < num_keys; i++)
                {
                        errno = 0;

                        new_record->data[i] = strtoul
			(start_ptr, &start_ptr, 10);

                        if(errno == ERANGE)
                        printf("Number out of range");

                }

                key = new_record->data[join_key_ndx];
                probe(new_record, key, outstream);
        }

        //pthread_exit(NULL);
        //printf("Worker %d, Thread %ld finished parititioning %d outer lines\n", worker_rank, (long)pthread_self(), num_lines);

}

void *partitionInnerRelation (void *arg)
{
	//printf("Worker %d in partition inner relation\n", worker_rank);

        long fsize;
        long dangler;
        long chunk_size;
        char line[1024];
	void *return_val;

        int num_threads = 1;
        Args args[num_threads];
        void *statuses[num_threads];
        pthread_t tids[num_threads];

	//time_t startTime = time(NULL);
	//pthread_join(inner_relation->ngbr_tid, &return_val);
	//char *relation = *((char**) return_val);
	char *relation = (char *) arg;	
		
        if(isEmpty(relation))
        {
		//printf("Worker %d, inner relation is empty; sending a signal\n", worker_rank);
                pthread_mutex_lock(&mutex);
                inner_relation->isEmpty = 1;
		pthread_cond_signal(&cond);
                pthread_mutex_unlock(&mutex);
		return NULL;
	}

        fsize = strlen(relation);

        chunk_size = fsize / num_threads;
        dangler = fsize % num_threads;
	
	//printf("WORKER %d, SIZE OF INNER RELATION: %ld\n", worker_rank, fsize);
        //printf("WORKER %d, CHUNK SIZE OF INNER RELATION: %ld\n", worker_rank, chunk_size);

        for(int i = 0; i < num_threads; i++)
        {
                args[i].instream = relation;
                args[i].chunk_size = chunk_size;
                args[i].offset = i * chunk_size;

		if(i == num_threads - 1)
                {
                        args[i].chunk_size += dangler;
                        partitionInnerRelationHelper(&args[i]);
                }

                else
                {
                        if(pthread_create(&tids[i], NULL,
                        partitionInnerRelationHelper, &args[i]))
			systemError((char*)"Could not create new thread");

			//printf("Thread %ld created new inner thread %ld\n", (long)pthread_self(), (long)tids[i]);
		        //pthread_join(tids[i], &statuses[i]);
                }
        }

	for(int i = 0; i < num_threads - 1; i++)
	{
		pthread_join(tids[i], &statuses[i]);
	}

	//time_t endTime = time(NULL);

        //free(relation);
	
	//char *task = (char*) "INNER PARTITIONING";
        //getUsage(task);

        pthread_mutex_lock(&mutex);
        pthread_cond_signal(&cond) ;
        pthread_mutex_unlock(&mutex) ;

	//printf("WORKER %d FINISHED PARTITIONING INNER RELATION IN %d SECONDS\n", worker_rank, endTime - startTime);

        return NULL;
}

void *partitionInnerRelationHelper (void *arg)
{
	//printf("Worker %d, Thread %ld is innerRelationHelper\n", worker_rank, (long)pthread_self());

	MAP::accessor a;

	char *start_ptr;
	char *end_ptr;
        ulong key;

	Args args       = *((Args *) arg);
        char *relation  = (char *) args.instream;
	long chunk_size = (long) args.chunk_size;
	long offset     = (long) args.offset;

	short num_keys = inner_relation->num_keys;
        short join_key_ndx = inner_relation->join_keys_ndxs[0];

	start_ptr  = relation;
	start_ptr += offset; 

	end_ptr  = start_ptr;
	end_ptr += chunk_size;

	while (*end_ptr != '\0' && *end_ptr != '\n')
	{
		end_ptr++;
	}

	end_ptr -= 2;
	long num_lines = 0;
	short i;

	while (start_ptr < end_ptr)
        {
                Record *new_record = createNewRecord(num_keys);
		num_lines++;

		for(i = 0; i < num_keys; i++)
                {
                        errno = 0;

                        new_record->data[i] = strtoul(start_ptr, &start_ptr, 10);

                        if(errno == ERANGE)
                        printf("Number out of range");

                }
				
		MAP::accessor a;

                key = new_record->data[join_key_ndx];	
                (inner_relation->hash_table).insert(a, key);
                (a->second).push_back(new_record);
        
		a.release();
	}

	//pthread_exit(NULL);
	//printf("Worker %d, Thread %ld finished parititioning %d inner lines\n", worker_rank, (long)pthread_self(), num_lines);
}

void probe (Record *record1, ulong key, FILE *stream)
{
	std::vector<Record*> matches;
	MAP::iterator pair;
	MAP::accessor a;
	Record *record2;	

	if(inner_relation->hash_table.find(a,key))
        {
        	matches = a->second;

		BOOST_FOREACH(record2,matches)
                {
                        matchRecord(record1,record2,stream);

                        //free(record2->data);
                        //free(record2);
                }
	}

	a.release();
}

void matchRelations ()
{
	//printf("Worker %d matching relations\n", worker_rank);
	std::vector<Record*> matches;
	MAP::iterator pair;
	ulong key;

	output->stream = fopen(output->filename, "w+");

        fprintf(output->stream, "<empty result>\n");
        fseek(output->stream, 0L, SEEK_SET);

	int iter = 0;

	printf("worker %d started matching\n", worker_rank);

	//tbb::parallel_for (outer_relation->hash_table.range(), 
	//		     [](const MAP::range_type &r) 
	//{
		//for(pair = r.begin(); pair != r.end(); pair++)

		for (pair  =  outer_relation->hash_table.begin();
		pair != outer_relation->hash_table.end(); pair++)
		{
			key = pair->first;

			MAP::accessor a;

	        	if(inner_relation->hash_table.find(a,key))
			{
				matches = a->second;
				matchRecords(pair->second, matches);
			}
		
			a.release();
		}
	//});
	//printf("worker %d finished matching\n", worker_rank);

	fclose(output->stream);
}

void matchRecords (std::vector<Record*> list1, std::vector<Record*> list2)
{
	Record *record1, *record2;

	BOOST_FOREACH(record1, list1)	
	{
		BOOST_FOREACH(record2, list2)
		{
			matchRecord(record1,record2,output->stream);

			//free(record2->data);
			//free(record2);
		}

		//free(record1->data);
		//free(record1);
	}
}

void matchRecord (Record *record1, Record *record2, FILE *stream)
{
	short ndx1, ndx2;
	short trueMatch = 1;

	for(short i = 0; i < output->num_join_keys; i++)
	{
		ndx1 = outer_relation->join_keys_ndxs[i];
		ndx2 = inner_relation->join_keys_ndxs[i];

		if(record1->data[ndx1] != record2->data[ndx2])
		{
			trueMatch = 0;
		}
	}

	if(trueMatch)	
	{
		writeRecord(record1, record2, stream);	
	}
	
}

void writeRecord (Record* record1, Record* record2, FILE *stream)
{
        ulong field;
        short outer_num_keys = outer_relation->num_keys;

        for(short i = 0; i < output->num_cols; i++)
        {
                if(output->order[i] < outer_num_keys)
		{
			/* read data from the inner relation */
                        field = record1->data[output->order[i]];
		}
                
		else
		{
			/* read data from the outer relation */
                    	field = record2->data[output->order[i] - outer_num_keys];
		}

                //fprintf(output->stream, "%u ", field);
		fprintf(stream, "%u ", field);
	}

	//fprintf(output->stream, "\n");
	fprintf(stream, "\n");
}


Record *createNewRecord (short num_keys)
{
        Record *r;

        if(!(r       = (Record *) malloc(sizeof(Record)))    ||
         //!(r->data = (uint *) malloc(sizeof(uint) * num_keys)))
           !(r->data = (ulong *) malloc(sizeof(ulong) * num_keys)))
	{
               	systemError((char*)"Could not create new record\n");
        }

        return r;
}

void freeRecord (Record **record)
{
	Record *ptr = *record;
	free(ptr->data);
	free(ptr);
}

void mergeKeys (char **my_keys[], char *ngbr_keys[], 
	    short final_num_keys, char *final_keys[])
{
        short i = 0, j = 0, k = 0;
        char **merged_keys;
	short *final_order;

        short outer_num_keys = outer_relation->num_keys;
        short inner_num_keys = inner_relation->num_keys;
        short total_num_keys = output->num_cols;

        if(!(merged_keys = (char **) malloc(sizeof(char *) * total_num_keys)))
        {
                systemError((char*) "Could not allocate merged_keys array");
        }

	if(!(output->order = (short *) malloc(sizeof(short) * total_num_keys)))
        {
                systemError((char*) "Could not allocate output_order array");
        }

	if(final_keys && !(final_order = (short *) malloc(sizeof(short) * final_num_keys)))
	{
		systemError((char*) "Could not allocate final_order array");
	}

        while (i < outer_num_keys && j < inner_num_keys)
        {
                short diff = strcmp((*my_keys)[i], ngbr_keys[j]);

                if(diff < 0)
                {
                        output->order[k] = i;
                        merged_keys[k++] = (*my_keys)[i++];
                }

                else if (diff > 0)
                {
                        output->order[k] = outer_num_keys + j;
                        merged_keys[k++] = ngbr_keys[j++];
                }

                else
                {
                        output->order[k] = outer_num_keys + j;
                        merged_keys[k++] = ngbr_keys[j++];
                        i++;
                }
        }

	while (i < outer_num_keys)
        {
                output->order[k] = i;
                merged_keys[k++] = (*my_keys)[i++];
        }

	while (j < inner_num_keys)
        {
                output->order[k] = outer_num_keys + j;
                merged_keys[k++] = ngbr_keys[j++];
        }

	if(final_keys != NULL)
	{
		k = 0;

		for(i = 0; i < final_num_keys; i++)
		for(j = 0; j < total_num_keys; j++)
		{
			if(!strcmp(final_keys[i], merged_keys[j]))
			{
			  final_order[k++] = output->order[j];
			}
		}

		output->num_cols = final_num_keys;
		
		free(output->order);
		output->order = final_order;
	}

	/*for(int i = 0; i < outer_num_keys; i++)
                free(&(*my_keys[i]));

        free(*my_keys);*/
        *my_keys = merged_keys;
}


void initDatastructures ()
{
        outer_relation  = new OuterRelation();
        inner_relation  = new InnerRelation();
	output          = new Output();
	output->stream  = NULL;
}

void getUsage (char *task)
{
	struct rusage usage;
	char about_task[100];
	FILE *stream;

	if(!(stream = fopen("Profiling.txt","w+")))
	systemError((char*)"Could not create Profiling.txt");

	sprintf(about_task, "PROFILING INFO FOR TASK %s\n", task);
	fprintf(stream, "%s", about_task);
	
        getrusage(RUSAGE_THREAD, &usage);
        
	fprintf(stream, "Number of pages faults without IO activity: %ld\n", usage.ru_minflt);
	fprintf(stream, "Number of pages faults with IO activity: %ld\n\n", usage.ru_minflt);
	fclose(stream);
	
}

void clean ()
{
        delete(outer_relation);
       	delete(inner_relation);
        delete(output);

	//if(pthread_cancel(outer_relation->tid) < 0)
        //printf("ERROR killing outer thread: %s\n", strerror(errno));

        //if(pthread_cancel(inner_relation->tid) < 0)
        //printf("ERROR killing inner thread: %s\n", strerror(errno));

        //if(pthread_cancel(outer_relation->subQ_tid) < 0)
        //printf("ERROR killing subQ thread: %s\n", strerror(errno));

        //if(pthread_cancel(inner_relation->ngbr_tid) < 0)
        //printf("ERROR killing ngbr thread: %s\n", strerror(errno));
}


