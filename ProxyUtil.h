#ifndef _PROXY_UTIL_H_
#define _PROXY_UTIL_H_

#include "QueryPlanner.h"

#include "Main.h"


int8_t *createFinalKeysMessage (Query *Q, int *message_len);
int8_t *createSubQMessage(Node *node, int *message_len);
char *readFile(char *filename);

void sendSubQToWorker(Query *Q, Node *node, int *num_workers_to_reply);

#endif
