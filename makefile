# $@ refers to the literal before :
# $^ refers to the literal after :
# $< refers to the first literal in the dependency list

# Rules to produce the targets

all: DREAM


DREAM: Main.o BasicHashJoin.o Client.o Errors.o MiscUtil.o Optimizer.o OptimizerWrapper.o PlanCostEstimator.o Proxy.o ProxyUtil.o QueryPlanner.o Worker.o WorkerUtil.o Wrapper.o
	mpicxx Main.o BasicHashJoin.o Client.o Errors.o MiscUtil.o Optimizer.o OptimizerWrapper.o PlanCostEstimator.o Proxy.o ProxyUtil.o QueryPlanner.o Worker.o WorkerUtil.o Wrapper.o -g -Wall -Wextra -ltbb -pthread -Werror -o dream

# Rules to produce the object files 
Main.o: Main.c Main.h ProxyUtil.h WorkerUtil.h MiscUtil.h Client.h Proxy.h Worker.h Wrapper.h
	mpicc -c -g Main.c -std=c99 -pthread  

BasicHashJoin.o: BasicHashJoin.cpp BasicHashJoin.h Errors.h
	mpicc -c -g -std=c++0x BasicHashJoin.cpp -pthread

Client.o: Client.c Client.h 
	mpicc -c -g Client.c -std=c99

Errors.o: Errors.c Errors.h
	mpicc -c -g Errors.c -std=c99

MiscUtil.o: MiscUtil.c MiscUtil.h QueryPlanner.h
	mpicc -c -g MiscUtil.c -std=c99

Optimizer.o: Optimizer.cpp Optimizer.h PlanCostEstimator.h
	mpicc -c -g Optimizer.cpp

OptimizerWrapper.o: OptimizerWrapper.cpp OptimizerWrapper.h Optimizer.h Structs.h
	mpicc -c -g OptimizerWrapper.cpp

PlanCostEstimator: PlanCostEstimator.cpp PlanCostEstimator.h QueryPlanner.h Structs.h Errors.h 
	mpicc -c -g -std=c++11 PlanCostEstimator.cpp

Proxy.o: Proxy.c Proxy.h
	mpicc -c -g Proxy.c -std=c99

ProxyUtil.o: ProxyUtil.c ProxyUtil.h QueryPlanner.h
	mpicc -c -g ProxyUtil.c -std=c99

QueryPlanner.o: QueryPlanner.c QueryPlanner.h OptimizerWrapper.h Errors.h
	mpicc -c -g QueryPlanner.c -std=c99 

Worker.o: Worker.c Worker.h
	mpicc -c -g Worker.c -std=c99 

WorkerUtil.o: WorkerUtil.c WorkerUtil.h QueryPlanner.h
	mpicc -c -g WorkerUtil.c -std=c99

Wrapper.o: Wrapper.cpp Wrapper.h BasicHashJoin.h
	mpicc -c -g Wrapper.cpp 

# remove the targets
.PHONY: clean

clean:
	rm -f *.o core

rebuild: clean all

