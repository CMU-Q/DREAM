#include "mpi.h"
#include "QueryPlanner.h"

#include "Main.h"

int MPI_URecv(char **buf, MPI_Datatype datatype, int source, int tag,
             			MPI_Comm comm, MPI_Status *status);
