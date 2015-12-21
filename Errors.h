#ifndef _ERRORS_H_
#define _ERRORS_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <error.h>
#include <netdb.h>
#include <signal.h>
#include <pthread.h>
#include <time.h>
#include <assert.h>
#include <sys/time.h>
#include <sys/resource.h>
#include "Structs.h"

char parseChar(char *message);
short parseShort(char *message);
int parseInt(char *message);

void parseError(char *error_msg);
void socketError(char *error_msg);
void systemError(char *error_msg);
void applicationError(char *error_msg);


#endif
