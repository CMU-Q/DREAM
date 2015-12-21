#include "Client.h"
extern int debug_level;

/*
	Sends the query to the PROXY and receives the result after that
*/
void doClient() 
{
	system("echo 'client was here' > client.txt"); // AISHA testing where client is located
	
	MPI_Status status;
	char *query = NULL;
	char *result = NULL;

	query = readFile(QUERY_FILE);
		
	MPI_Send(query, strlen(query), MPI_CHAR, PROXY, Q_TAG, MPI_COMM_WORLD);
	//aisha debug_print(debug_level, COMM_LOG, "%s\n", "MPI_SEND IN CLIENT");
	free(query);
	
	MPI_URecv(&result, MPI_CHAR, PROXY, Q_RESULT_TAG, MPI_COMM_WORLD, &status);

	//aisha debug_print(debug_level, COMM_LOG, "%s\n", "MPI_URecv IN CLIENT");

	//printf("\n*****   Result:   *****\n%s\n**** End of Result ****\n",result);
}
