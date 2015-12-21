#include "MiscUtil.h"
#include <stdlib.h>

int MPI_URecv(char **buf, MPI_Datatype datatype, int source, int tag,
				MPI_Comm comm, MPI_Status *status)
{
	MPI_Status s;
	extern int debug_level;
 	int message_len;

	char error_msg[MPI_MAX_ERROR_STRING];
	int error_msglen;
	
	int ret = MPI_Probe(source, tag, comm, &s);
	//debug_print(debug_level, COMM_LOG, "%s\n", "MPI_Probe"); aisha
	MPI_Error_string(ret, error_msg, &error_msglen);

	MPI_Get_count(&s, datatype, &message_len);

	if((*buf = (char *)malloc(message_len * sizeof(char))) == NULL)	{
		systemError("Can not allocate buffer in URecv");					
		//debug_print(debug_level, ERROR_LOG, "%s\n", "Can not allocate buffer in URecv"); aisha
	}

	memset(*buf, 0, sizeof(*buf));
	
	int result = MPI_Recv(*buf, message_len, datatype, source, tag, comm, status);
	
	if (result != MPI_SUCCESS) 
		printf("MPI URECV FAILED\n\n");
		
	//debug_print(debug_level, COMM_LOG, "%s\n", "MPI_Recv"); aisha

	return message_len;
}
