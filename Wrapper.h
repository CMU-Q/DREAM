#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>

void joinHelper (int my_worker_id, short *my_num_keys, char **my_keys[],
         short ngbr_num_keys, char *ngbr_keys[], char *ngbr_result_set,
	 short final_num_keys, char *final_keys[], pthread_t subQ_tid, pthread_t ngbr_tid);

#ifdef __cplusplus
	}
#endif

