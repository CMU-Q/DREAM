#include "OptimizerWrapper.h"
#include "Optimizer.h"

/**
 * Header with the Optimizer and statistics-loading functionalities.
 * 
 */

//aisha - this function can be moved directly to Optimizer
void getOptimalPlan (Query **Q)
{
	getOptimalPlanHelper(Q);
} 

void loadStats ()
{
	loadResultSizeStats(STAT_FILE_1);
	loadScanSizeStats(STAT_FILE_2);
}
 
