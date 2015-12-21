#include "Wrapper.h"
#include "BasicHashJoin.h"

void joinHelper (int my_worker_id, short *my_num_keys, char **my_keys[],
	short ngbr_num_keys, char *ngbr_keys[], char *ngbr_result_set,
	short final_num_keys, char *final_keys[], pthread_t subQ_tid, pthread_t ngbr_tid)
{

//my_rank, &my_num_keys, &my_keys, ngbr_num_keys, ngbr_keys, 
//				ngbr_result_set, 0, NULL, subQ_tid, prTids[num_results_recvd]);

//&my_keys, ngbr_result_set, 0, NULL, subQ_tid, prTids[num_results_recvd]);

/*aisha start
	printf("&join helper&\n");
	printf("WORKER ID: %d\n", my_worker_id);
	printf("\t my_num_keys: %hd\n\t\t", *my_num_keys);
	for(int k = 0; k<*my_num_keys; k++){
		printf("%s, ", (*my_keys)[k]);		
	}
	printf("\n");

	printf("\t ngbr_num_keys: %hd\n\t\t", ngbr_num_keys);
	for(int k = 0; k<ngbr_num_keys; k++){
		printf("%s, ", ngbr_keys[k]);		
	}
	printf("\n");
//aisha end*/

	joinResultSets (my_worker_id, my_num_keys, my_keys, ngbr_num_keys,
		ngbr_keys, ngbr_result_set, final_num_keys, final_keys, subQ_tid, ngbr_tid);
}

