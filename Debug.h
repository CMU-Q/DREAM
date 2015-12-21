#ifdef __cplusplus
extern "C" 
{
#endif



#define ERROR_LOG 	1
#define COMM_LOG 	2
#define Q_PLAN_LOG	3

#define DEBUG 1

#ifdef DEBUG
#define DEBUG_TEST 1
#else
#define DEBUG_TEST 0
#endif


#define debug_print(level, logtype, fmt, ...) \
	do	{	if (DEBUG_TEST) \
			{ \
				if (level==0) break; \
				else if (level==1) { switch(logtype) {	\
					case ERROR_LOG:	printf("%s:%d: ERROR_LOG: %s(): " fmt, __FILE__, \
									__LINE__, __func__, __VA_ARGS__); break;\
					}			}			\
				else if (level<=2) { switch(logtype) {	\
					case ERROR_LOG:	printf("%s:%d: ERROR_LOG: %s(): " fmt, __FILE__, \
									__LINE__, __func__, __VA_ARGS__); break; \
					case COMM_LOG:	printf("%s:%d: COMM_LOG: %s(): " fmt, __FILE__, \
									__LINE__, __func__, __VA_ARGS__); break;\
					}	}\
				else if (level<=3 && level>=1) { switch(logtype) {	\
					case ERROR_LOG:	printf("%s:%d: ERROR_LOG: %s(): " fmt, __FILE__, \
									__LINE__, __func__, __VA_ARGS__); break;\
					case COMM_LOG:	printf("%s:%d: COMM_LOG: %s(): " fmt, __FILE__, \
									__LINE__, __func__, __VA_ARGS__); break;\
					case Q_PLAN_LOG:	printf("%s:%d: Q_PLAN_LOG: %s(): " fmt, __FILE__, \
									__LINE__, __func__, __VA_ARGS__); break;\
					}	}\
			} \
		} while (0)
					
#ifdef __cplusplus
}
#endif

 