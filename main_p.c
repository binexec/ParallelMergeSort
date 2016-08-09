/*Main entry point for PARALLEL mergesort*/
#include <mpi.h>
#include "mergesort.h"
#include "userdef.h"

#define MIN_ARGS 3
#define NO_OUTPUT_ARG "-benchmark"
#define NO_VERIFY_ARG "-noverify"

int p;						//total number of processes

//Struct containing the interpreted input arguments for the main master process
typedef struct{
	DATA_TYPE *data;		//Pointer to the data that was read in from the input file
	char *outfile;			//Output filename
	int elements;			//How many elements are in data?
	int output_enabled;		//Do we output the results once sorted?
	int no_verify;			//Skip the verification step after sorting?
}ParsedArgs;

//A struct of arguments instructing a node where to send/receive data during phase 2 merge
typedef struct{
	enum {NONE = 0, SEND, RECV} op;		//Should the node send data, receive data, or do nothing?
	int elements;						//# of elements to receive or send for this operation
	int target;							//The node to send/receive from
	int completed;						//Does node 1 have a completed merged list?
} MergeInstr;

//Parase the raw input arguments into appropriate settings used by the program
ParsedArgs parseArgs(int argc, char *argv[])
{
	int i;
	ParsedArgs pargs;
	
	//Make sure we have enough arguments first
	if(argc < MIN_ARGS)
	{
		printf("Insufficient arguments!\n");
		printf("Usage: psort infile outfile <optional parameters> \n");
		printf("Optional parameters: %s %s\n", NO_OUTPUT_ARG, NO_VERIFY_ARG);
		MPI_Abort(MPI_COMM_WORLD, -1);
	}

	//Default values for args to be returned
	pargs.output_enabled = 1; 
	pargs.no_verify = 0;
	
	//Treat the argument 1 and 2 as input and output filenames
	printf("Input file: \"%s\"\n", argv[1]);
	printf("Output file: \"%s\"\n", argv[2]);
	
	//Parse additional arguments specified by the user
	if(argc > MIN_ARGS)
	{
		//Treat arguments 2 to n-2 as all possible parameters and scan through them
		for(i=MIN_ARGS; i<argc; i++)
		{
			if(strncmp(argv[i], NO_OUTPUT_ARG, strlen(NO_OUTPUT_ARG)) == 0)
			{
				pargs.output_enabled = 0; 
				printf("Found \"%s\" argument; output disabled\n", NO_OUTPUT_ARG);
			}
			
			else if(strncmp(argv[i], NO_VERIFY_ARG, strlen(NO_VERIFY_ARG)) == 0)
			{
				pargs.no_verify = 1; 
				printf("Found \"%s\" argument; sorting verification disabled\n", NO_VERIFY_ARG);
			}
			
			else
				printf("Ignored unknown parameter \"%s\"\n", argv[i]);
		}
	}
	
	//Read data from the input file, and set the output file
	pargs.data = readDataFromFile(argv[1], &pargs.elements);	
	pargs.outfile = argv[2];

	//Check if the file read was sucessful
	if(pargs.data == NULL)
	{
			printf("readDataFromFile returned a NULL pointer!");
			MPI_Abort(MPI_COMM_WORLD, -1);
	}
	
	return pargs;
}

//Calculates the count and displacement for each process used by mpi_scatterv
void calcCountDisp(int *node_status, int *node_disp, int p, int elements)
{
	int i, j;
	int elements_per_node = elements/p;
	
	if(elements < p)
	{
		memset(node_disp, 0, sizeof(int)*p);	//Set all node_disp[i] = 0
		memset(node_status, 0, sizeof(int)*p);	//Set all node_status[i] = 0
		node_status[0] = elements;
		return;
	}
	
	for(i=0; i<p; i++)
	{
		if(i == p-1)
			node_status[i] = elements - i*elements_per_node;
		else
			node_status[i] = elements_per_node;
		
		node_disp[i] = i*elements_per_node;
	}
}

//Calculates a new global snapshot of how much data is at each process at the next iteration,
//and instructs the calling process where to send/receive its sorted sublist
MergeInstr planMerge(int *data_remaining, int id)
{
	int i, j;
	int has_data_count = 0, handled = 0;
	MergeInstr instr;
	
	//Default values
	instr.op = NONE;
	instr.elements = 0;
	instr.target = -1;
	instr.completed = 0;
	
	//Count how many nodes have data during this iteration
	for(i=1; i<p; i++)
	{
		if(data_remaining[i] > 0)
			has_data_count++;
	}
	has_data_count++;		//Add 1 to count since node 0 always has data and is ignored in the previous loop.	
	
	if(has_data_count == 1)
	{
		instr.op = NONE;
		instr.completed = 1;
		return instr;
	}
	
	for(i=0; i<p; i++)
	{
		//No need to process nodes that do not have data left
		if(data_remaining[i] <= 0)
			continue;
		
		//If all nodes have data, then this must be the first iteration.
		if(has_data_count == p)
		{
			//If this is the last node and there are odd number of nodes, don't merge on the first iteration
			if(i == p-1 && p%2 == 1)
			{
				if(i == id )
				{
					instr.op = NONE;
					instr.completed = 0;
				}
			}
			
			//If i'm an odd numbered node, send data to node i-1
			else if(i%2 == 1)
			{
				if(i == id)
				{
					instr.op = SEND;
					instr.target = i-1;
					instr.elements = data_remaining[i];
					instr.completed = 0;
				}
				data_remaining[i] = 0;
			}
			
			//If i'm an even numbered node, receive data from node i+1
			else if (i%2 == 0)
			{
				if(i == id)
				{
					instr.op = RECV;
					instr.target = i+1;
					instr.elements = data_remaining[i+1];
					instr.completed = 0;
				}
				data_remaining[i] += data_remaining[i+1];
			}
		}
		
		//Any other iterations with a odd number of nodes having data
		else if (has_data_count % 2 == 1)
		{
			handled++;
			
			//If i'm the last node with data, do nothing this iteration.
			if(handled == has_data_count)
			{
				if(i == id)
				{
					instr.op = NONE;
					instr.completed = 0;
				}
			}
			
			//On a node with data, if handled is odd, this will be a receiving node for the next iteration
			else if(handled % 2 == 1)
			{
				//Find the next node with data 
				for(j=i+1; j<p; j++)
					if(data_remaining[j] > 0) break;
				
				data_remaining[i] += data_remaining[j];
				
				if(i == id)
				{
					instr.op = RECV;
					instr.target = j;
					instr.elements = data_remaining[j];
					instr.completed = 0;
				}
			}
			//On a node with data, if handled is even, this will be a sending node for the next iteration
			else if(handled % 2 == 0)
			{
				if(i == id)
				{
					//Find the next node with data 
					for(j=i-1; j>=0; j--)
						if(data_remaining[j] > 0) break;
				
					instr.op = SEND;
					instr.elements = data_remaining[i];
					instr.target = j;
					instr.completed = 0;
				}
				data_remaining[i] = 0;
			}
		}
		
		//Any other iterations with an even number of nodes having data
		else if (has_data_count % 2 == 0)
		{
			handled++;
			
			//On a node with data, if handled is odd, this will be a receiving node for the next iteration
			if(handled % 2 == 1)
			{
				//Find the next node with data 
				for(j=i+1; j<p; j++)
					if(data_remaining[j] > 0) break;
				
				data_remaining[i] += data_remaining[j];
				
				if(i == id)
				{
					instr.op = RECV;
					instr.elements = data_remaining[j];
					instr.target = j;
					instr.completed = 0;
				}
			}
			//On a node with data, if handled is even, this will be a sending node for the next iteration
			else if(handled % 2 == 0)
			{
				if(i == id)
				{
					//Find the next node with data 
					for(j=i-1; j>=0; j--)
						if(data_remaining[j] > 0) break;
				
					instr.op = SEND;
					instr.elements = data_remaining[i];
					instr.target = j;
					instr.completed = 0;
				}
				data_remaining[i] = 0;
			}
		}
	}
	return instr;
}

//Receive a sorted sublist from another process, and merge it with its own sublist
DATA_TYPE* recvAndMerge(DATA_TYPE *data, int n, MergeInstr instr)
{
	//printf("NODE: %d Iter: %d OP: RECV, Target: %d Before: %d Elements to S/R: %d After: %d Comp: %d\n", id, i++, instr.target, node_status[id], instr.elements, node_status_next[id], instr.completed);
	//printf("Node: %d, RECV buffer size: %d\n", id, node_status[id] + instr.elements);

	DATA_TYPE *tdata1;			//Temporary buffer to hold the current data at the node
	DATA_TYPE *tdata2;			//Temporary buffer to hold the incoming data 
	MPI_Status recv_status;	
	
	tdata1 = (DATA_TYPE*) malloc(sizeof(DATA_TYPE)*n);					
	tdata2 = (DATA_TYPE*) malloc(sizeof(DATA_TYPE)*instr.elements);		

	memcpy(tdata1, data, (sizeof(DATA_TYPE)*n));	//tdata1 = data
	
	//Receive the expected data
	MPI_Recv(tdata2, instr.elements, MPI_DATA_TYPE, instr.target, 0, MPI_COMM_WORLD, &recv_status);
	
	//Allocate a bigger chunk of memory for "data" to hold the merged results of tdata1 and tdata2
	free(data);
	data = (DATA_TYPE*) malloc(sizeof(DATA_TYPE)*(n + instr.elements));	
	
	//Merge "tdata1" and "tdata2" together back into "data"
	Merge(data, tdata1, n, tdata2, instr.elements, sizeof(DATA_TYPE), compFunc);
	
	free(tdata1);
	free(tdata2);
	
	return data;
}

//Verify if a given array is indeed sorted
int checkSorted(DATA_TYPE *data, int n, int (*comp)(void *, void *))
{
	int i;
	int sorted = 1;
	
	for(i=1; i<n; i++)
	{
		if(((*comp)(&data[i-1], &data[i])) == 1)		//if(data[i-1] > data[i])
		{
			sorted = 0;
			break;
		}
	}
	printf("VERIFICATION: The array %s sorted!\n", sorted? "is":"is NOT");
	return sorted;
}

//The code ran by the master process (process id 0)
double masterProcess(int id, ParsedArgs args)
{
	double elapsed_time;
	
	//Needed by master node only
	int output_enabled = args.output_enabled;	//If we're running a benchmark, don't output the result
	int elements = args.elements;				//How many elements are in the expected data set
	int no_verify = args.no_verify;				//Skip verification after sorting completes?
	char *outfile = args.outfile;				//Filename of the output file
	DATA_TYPE *all_data = args.data; 			//The master's initial data buffer to hold generated data or data read from file
	
	//Variables related to send and receiving data
	int *node_disp;				//An array used by scatterv to know where to break the data for distribution
	int *node_status;			//An array keeping track of the data size of each node's sorted sublist at the CURRENT time
	int *node_status_next;		//An array keeping track of the data size of each node's sorted sublist AFTER one iteration of phase 2 merge
	MergeInstr instr;			//A struct telling a node where to send its sorted sublist, or who to receive it from
	MPI_Status recv_status;	
	
	//Data related variables for all nodes
	int filesize;
	int i,j, retval;
	DATA_TYPE *data, *tdata1, *tdata2;	

	
	/*****************************************************/
	/*  Distributing Data and Sorting Initial Sublists   */
	/*****************************************************/	
	
	//Start recording the sorting time.
	printf("\n======BEGIN=======\n\n");
	elapsed_time = -MPI_Wtime();		
	
	//If there's only one process available, just do a simple serial mergesort
	if(p == 1)
	{
		printf("NOTE: Only 1 process found. Running a serial version of Mergesort instead\n");
		MergeSort(all_data, elements, sizeof(DATA_TYPE), compFunc);

		//Stop recording the sorting time
		elapsed_time += MPI_Wtime();			

		//Verify the data is indeed sorted
		if(!no_verify)
			checkSorted(all_data, elements, compFunc);
		
		//Outputs the results if needed
		if(output_enabled)
			writeDataToFile(all_data, elements, outfile);
		
		//Cleanup and exit
		free(all_data);
		return elapsed_time;
	}
	
	//Proceeding with parallel mergesort if there's more than 1 processes
	
	node_status = (int*) malloc(sizeof(int)*p);
	node_disp = (int*) malloc(sizeof(int)*p);
	node_status_next = (int*) malloc(sizeof(int)*p);
	
	//Broadcast the number of elements to all other processes so they can allocate a memory buffer for the task.
	printf("***Broadcasting Size!***\n");
	MPI_Bcast(&elements, 1, MPI_INT, 0, MPI_COMM_WORLD);
	
	//Calculating how many elements each process will be receiving, and its displacement
	calcCountDisp(node_status, node_disp, p, elements);
	data = (DATA_TYPE*) malloc(sizeof(DATA_TYPE) * node_status[id]);
	
	//Send a chunk of the data file to all nodes
	printf("***Distributing Data!***\n");
	MPI_Scatterv(all_data, node_status, node_disp, MPI_DATA_TYPE, data, node_status[id], MPI_DATA_TYPE, 0, MPI_COMM_WORLD);
	free(all_data);
	free(node_disp);
	
	//Calling mergesort to sort the received data portion. 
	printf("***Sorting Sublists!***\n");
	MergeSort(data, node_status[id], sizeof(DATA_TYPE), compFunc);
	MPI_Barrier(MPI_COMM_WORLD);
	
	/*****************************************************/
	/* Beginning merging sublists across nodes (phase 2) */
	/*****************************************************/
	
	printf("***Phase 2 Merging Begins!***\n");
	memcpy(node_status_next, node_status, sizeof(int)*p);
	instr.completed = 0;
	i = 0;

	while(!instr.completed)
	{
		printf("***Phase 2 iteration %d***\n", i++);

		//Process 0 will only receive data from other Processes at any iteration
		instr = planMerge(node_status_next, id);
		if(instr.op == RECV)
			data = recvAndMerge(data, node_status[id], instr);
		
		//Update the current node statuses
		memcpy(node_status, node_status_next, sizeof(int)*p);
		
		//Wait for every node to complete this iteration 
		MPI_Barrier(MPI_COMM_WORLD);
	}
	
	//Stop recording the sorting time
	elapsed_time += MPI_Wtime();			

	//Verify the data is indeed sorted
	if(!no_verify)
		checkSorted(data, elements, compFunc);
	
	//Outputs the results if needed
	if(output_enabled)
		writeDataToFile(data, elements, outfile);
	
	free(data);
	free(node_status);
	free(node_status_next);
	return elapsed_time;
}

//The code ran by all other slave nodes
void slaveProcess(int id)
{
	int *node_disp;				//An array used by scatterv to know where to break the data for distribution
	int *node_status;			//An array keeping track of the data size of each node's sorted sublist at the CURRENT time
	int *node_status_next;		//An array keeping track of the data size of each node's sorted sublist AFTER one iteration of phase 2 merge
	MergeInstr instr;			//A struct telling a node where to send its sorted sublist, or who to receive it from	
	
	//Data related variables for all nodes
	int elements;
	int filesize;
	int i,j, retval;
	DATA_TYPE *data, *tdata1, *tdata2; 
	
	//Other initializations
	node_status = (int*) malloc(sizeof(int)*p);
	node_disp = (int*) malloc(sizeof(int)*p);
	node_status_next = (int*) malloc(sizeof(int)*p);
	
	//Wait for node 0 to broadcast the number of elements
	MPI_Bcast(&elements, 1, MPI_INT, 0, MPI_COMM_WORLD);
	
	//Calculate how much data everyone (including myself) will be receiving
	calcCountDisp(node_status, node_disp, p, elements);				
	data = (DATA_TYPE*) malloc(sizeof(DATA_TYPE) * node_status[id]);
	
	//Receive a chunk of data from master node 0
	MPI_Scatterv(NULL, node_status, node_disp, MPI_DATA_TYPE, data, node_status[id], MPI_DATA_TYPE, 0, MPI_COMM_WORLD);
	free(node_disp);
	
	//Calling mergesort to sort the received sublist. 
	MergeSort(data, node_status[id], sizeof(DATA_TYPE), compFunc);
	MPI_Barrier(MPI_COMM_WORLD);
	
	/*****************************************************/
	/* Beginning merging sublists across nodes (phase 2) */
	/*****************************************************/

	memcpy(node_status_next, node_status, sizeof(int)*p);
	instr.completed = 0;
	i = 0;
	
	while(!instr.completed)
	{
		instr = planMerge(node_status_next, id);
		
		//The node is instructed to receive a sorted sublist from another node
		if(instr.op == RECV)
			data = recvAndMerge(data, node_status[id], instr);
		
		//Every slave node must send their sorted sublist to another node at some iteration. Once send, the slave node will become idle/empty.
		else if(instr.op == SEND)
		{
			//printf("NODE: %d Iter: %d OP: SEND, Target: %d Before: %d Elements to S/R: %d After: %d Comp: %d\n", id, i++, instr.target, node_status[id], instr.elements, node_status_next[id], instr.completed);
			MPI_Send(data, node_status[id], MPI_DATA_TYPE, instr.target, 0, MPI_COMM_WORLD);
			free(data);	
		}
		
		//Update the current node statuses
		memcpy(node_status, node_status_next, sizeof(int)*p);
		
		//Wait for every node to complete this iteration 
		MPI_Barrier(MPI_COMM_WORLD);
	}
	
	free(node_status);
	free(node_status_next);
}

int main(int argc, char *argv[]) 
{
	int id;						//Rank of the current process
	double elapsed_time;		//Records how long the calculation took
	ParsedArgs parsed_args;		//Interpreted input arguments for the main process
	
	//Initialize MPI on all processes
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &id);
	MPI_Comm_size(MPI_COMM_WORLD, &p);
	
	//Run any initialization code the user has specified
	userInit();
	
	//Let rank 0 be the master process.
	if(id == 0)
	{
		//Parse the input arguments and read the data file specified
		parsed_args = parseArgs(argc, argv);
	
		//Start the Mergesort Master
		printf("Processes available: %d\n", p);
		elapsed_time = masterProcess(id, parsed_args);
		printf("Sorting took %lf seconds.\n", elapsed_time);
	}
	
	//Let all other ranks be worker processes
	else	
	{
		//Start the Mergesort Slave
		slaveProcess(id);
	}

	MPI_Finalize();
	return 0;
}