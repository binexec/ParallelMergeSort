/*Main entry point for PARALLEL mergesort*/

#include <stdlib.h>
#include <mpi.h>
#include "mergesort.h"

#define FILENAME 	"data.txt"			//Filename of the target data file
#define OUTPUT_PATH	"/home/bowenliu/csc462/"

typedef double DATA_TYPE;				//Change the data type for the values held in the database here
#define MPI_DATA_TYPE MPI_DOUBLE		//MPI data type must be equivalent to DATA_TYPE. A custom MPI data type might be needed.

typedef struct{
	enum {NONE = 0, SEND, RECV} op;
	int elements;	//# of elements to receive or send for this operation
	int target;
	int completed;
} MergeInstr;

int compfunc(DATA_TYPE *a, DATA_TYPE *b)
{
	DATA_TYPE deref_a = *a;
	DATA_TYPE deref_b = *b;
	
	//printf("COMPFUNC. A:%f, B:%f\n", deref_a, deref_b);
	
	if(deref_a < deref_b) 
		return -1;
	else if(deref_a > deref_b) 
		return 1;
	else 
		return 0;
}

int checkSorted(DATA_TYPE *data, int n)
{
	int i;
	int sorted = 1;
	
	for(i=1; i<n; i++)
	{
		if(data[i-1] > data[i])
		{
			sorted = 0;
			break;
		}
	}

	printf("The array %s sorted!\n", sorted? "is":"is NOT");
	return sorted;
}

void arrayCopy(int *dest, int *src, int n)
{
	int i;	
	for(i=0; i<n; i++)
	{
		dest[i] = src[i];
	}
}

/*Get the file size*/
size_t getFileSize(FILE *fp)
{
	size_t filesize;
	
	//Find the filesize
	fseek(fp, 0L, SEEK_END);
	filesize = ftell(fp);
	rewind(fp);					//Rewind the file pointer to beginning
	
	return filesize;
}

//Generate a data set of size n of DATA_TYPE
DATA_TYPE* generateData(int n)
{
	int i;
	double newval, scale;
	DATA_TYPE *data;
	
	//Create a buffer to hold the generated data
	data = (DATA_TYPE*) malloc(sizeof(DATA_TYPE)*n);
	
	//Seed the random number generator using current time
	srand((unsigned)time(NULL));
	
	//Start generating!
	for(i=0; i<n; i++)
	{
		//generate a random float
		scale = (double)rand();
		newval = ((double)rand() / (double)RAND_MAX) * scale;
		
		data[i] = newval;
	}
	
	printf("Generated %d elements\n", n);
	return data;
}

//Calculates the count and displacement used by mpi_scatterv
void calcCountDisp(int *send_count, int *send_disp, int p, int elements)
{
		int i, j;
		int elements_per_node = elements/p;
		
		if(elements < p)
		{
			memset(send_disp, 0, sizeof(int)*p);	//Set all send_disp[i] = 0
			memset(send_count, 0, sizeof(int)*p);	//Set all send_count[i] = 0
			send_count[0] = elements;
			return;
		}
		
		for(i=0; i<p; i++)
		{
			if(i == p-1)
				send_count[i] = elements - i*elements_per_node;
			else
				send_count[i] = elements_per_node;
			
			send_disp[i] = i*elements_per_node;
		}
}

MergeInstr planMerge(int *status, int id, int p)
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
		if(status[i] > 0)
			has_data_count++;
	}
	has_data_count++;		//Add 1 to count since node 0 always has data and is ignored in the previosu loop.	
	
	if(has_data_count == 1)
	{
		instr.op = NONE;
		instr.completed = 1;
		return instr;
	}
	
	for(i=0; i<p; i++)
	{
		//No need to process nodes that do not have data left
		if(status[i] <= 0)
			continue;
		
		//If all nodes have data, then this must be the first iteration.
		if(has_data_count == p)
		{
			//If this is the last node and there are even number of nodes, don't merge on the first iteration
			if(i == p-1 && p % 2 == 1)
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
					instr.elements = status[i];
					instr.completed = 0;
				}
				status[i] = 0;
			}
			
			//If i'm an even numbered node, receive data from node i+1
			else if (i%2 == 0)
			{
				if(i == id)
				{
					instr.op = RECV;
					instr.target = i+1;
					instr.elements = status[i+1];
					instr.completed = 0;
				}
				status[i] += status[i+1];
			}
		}
		
		//Any other iterations with a odd number of nodes having data
		else if (has_data_count % 2 == 1)
		{
			handled++;
			
			//If i'm the last node with data, do nothing for the next iteration.
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
					if(status[j] > 0) break;
				
				status[i] += status[j];
				
				if(i == id)
				{
					instr.op = RECV;
					instr.target = j;
					instr.elements = status[j];
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
						if(status[j] > 0) break;
				
					instr.op = SEND;
					instr.elements = status[i];
					instr.target = j;
					instr.completed = 0;
				}
				status[i] = 0;
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
					if(status[j] > 0) break;
				
				status[i] += status[j];
				
				if(i == id)
				{
					instr.op = RECV;
					instr.elements = status[j];
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
						if(status[j] > 0) break;
				
					instr.op = SEND;
					instr.elements = status[i];
					instr.target = j;
					instr.completed = 0;
				}
				status[i] = 0;
			}
		}
	}
	return instr;
}

int main(int argc, char *argv[]) 
{
	//MPI related variables
	int id, p;					//Rank of the current process, and total number of processes
	int *send_count;
	int *send_disp;
	int *merge_status;
	MergeInstr instr;
	MPI_Status recv_status;
	
	//Data variable for master node
	FILE *infile;
	DATA_TYPE *all_data; 
	
	//Data related variables for all nodes
	int elements;
	int filesize;
	int i,j, retval;
	DATA_TYPE *data; 
	
	//Debug variables
	FILE *dbgfp;
	char dbgfilename[10];

	//Initialize MPI on all processes
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &id);
	MPI_Comm_size(MPI_COMM_WORLD, &p);
	
	//Other Initializations that are done on all processes
	send_count = (int*) malloc(sizeof(int)*p);
	send_disp = (int*) malloc(sizeof(int)*p);
	merge_status = (int*) malloc(sizeof(int)*p);
	
	/*sprintf(dbgfilename, "%sout_%d", OUTPUT_PATH, id);
	dbgfp = fopen(dbgfilename, "w");
	
	if(dbgfp == NULL)
	{
		printf("Process %d: Could not open output file for writing!\n", id);
		return -1;
	}*/
	
	MPI_Barrier(MPI_COMM_WORLD);
	
	//Let rank 0 be the master process. 
	if(id == 0)
	{
		printf("Processes available: %d\n", p);

		/*
		//Open up the data file
		infile = fopen(FILENAME, "r");
		if(infile == NULL)
		{
			perror("Failed to open data file");
			return -1;
		}
		
		filesize = getFileSize(infile);
		
		//Get the size of the input file, and allocate the data buffer based on this size.
		//The resulting buffer will be a bit bigger than needed, but that's okay for now.
		all_data = (DATA_TYPE*) malloc(filesize);
		
		//Read the data file into the buffer until we hit end of the file
		for(elements = 0, retval = 1; retval > 0; elements++)
			retval = fscanf(infile, "%lf\n", &all_data[elements]);		//NOTE: Change the format identifier here if DATA_TYPE has changed!
		fclose(infile);
		
		//Trim the unused memory out of the data buffer.
		//The last element is also removed since the previous file reading method adds an extra zero element
		elements -= 1;
		all_data = realloc(all_data, sizeof(DATA_TYPE)*elements);
		
		printf("Read in %d elements from the data file %s\n", elements, FILENAME);
		
		*/
		
		/*Since Myth can't access files it seems, we'll temporarily just generate the values*/
		elements = 10000000;
		all_data = generateData(elements);
		
		printf("***Broadcasting Sizes!***\n");
		
		//Broadcast the number of elements to all other processes so they can allocate a memory buffer for the task.
		MPI_Bcast(&elements, 1, MPI_INT, 0, MPI_COMM_WORLD);
		
		//Calculating how many elements each process will be receiving, and its displacement
		calcCountDisp(send_count, send_disp, p, elements);
		data = (DATA_TYPE*) malloc(sizeof(DATA_TYPE) * send_count[id]);
		
		printf("***Distributing Data set!***\n");
		
		//Send a chunk of the data file to all nodes
		MPI_Scatterv(all_data, send_count, send_disp, MPI_DATA_TYPE, data, send_count[id], MPI_DATA_TYPE, 0, MPI_COMM_WORLD);
		free(all_data);
		free(send_disp);
		
		printf("***Sorting sublists...***\n");
		
		//Calling merge sort to sort the received data portion. 
		MergeSort(data, send_count[id], sizeof(DATA_TYPE), compfunc);
		MPI_Barrier(MPI_COMM_WORLD);		//Wait for everyone to complete sorting their sublists
		
		printf("***Phase 2 Merging Begins!***\n");
		
		/*for(i=0; i<send_count[id]; i++)
			fprintf(dbgfp, "%lf\n", data[i]);
			//printf("%lf\n", data[i]);
		fclose(dbgfp);*/
		
		//Beginning merging sublists across nodes
		arrayCopy(merge_status, send_count, p);
		
		instr.completed = 0;
		i = 0;
		
		while(!instr.completed)
		{
			printf("***Phase 2 iteration %d***\n", i++);
			instr = planMerge(merge_status, id, p);
			
			/*printf("Status of Node 0: ");
			for(j=0; j<p; j++)
			{
				printf("ID:%d,Size:%d; ", j, merge_status[j]);
			}
			printf("\n");*/
			
			//Process 0 will only receive data from other Processes at any iteration
			if(instr.op == RECV)
			{
				//printf("NODE: %d Iter: %d OP: RECV, Target: %d Before: %d Elements to S/R: %d After: %d Comp: %d\n", id, i-1, instr.target, send_count[id], instr.elements, merge_status[id], instr.completed);
				
				data = realloc(data, sizeof(DATA_TYPE)*(send_count[id] + instr.elements));
				//printf("Node: %d, RECV buffer size: %d\n", id, send_count[id] + instr.elements);
				
				if(data == NULL)
				{
					printf("Node %d: Realloc failed!\n", id);
					return -1;
				}
				
				MPI_Recv(&data[send_count[id]], instr.elements, MPI_DATA_TYPE, instr.target, 0, MPI_COMM_WORLD, &recv_status);
				
				//Merge(data, void *L, int left_n, void *R, int right_n, size_t data_size, int (*comp)(void *, void *));
				MergeSort(data, (send_count[id] + instr.elements), sizeof(DATA_TYPE), compfunc);	//Replace with MERGE instead!
			}

			arrayCopy(send_count, merge_status, p);
			
			//Wait for every node to complete this iteration 
			MPI_Barrier(MPI_COMM_WORLD);
		}
		
		checkSorted(data, elements);
		
		//printing all elements in the array once its sorted.
		/*for(i = 0; i<elements; i++) 
		{
			printf("%f\n", data[i]);
		}*/
		
		free(data);
	}
	
	//What to do for all other worker processes
	else
	{
		//Wait for node 0 to broadcast the number of elements
		MPI_Bcast(&elements, 1, MPI_INT, 0, MPI_COMM_WORLD);
		
		//Calculate how much data everyone (including myself) will be receiving
		calcCountDisp(send_count, send_disp, p, elements);				
		data = (DATA_TYPE*) malloc(sizeof(DATA_TYPE) * send_count[id]);
		
		//Receive a chunk of data from master node 0
		MPI_Scatterv(all_data, send_count, send_disp, MPI_DATA_TYPE, data, send_count[id], MPI_DATA_TYPE, 0, MPI_COMM_WORLD);
		free(send_disp);
		
		//Calling merge sort to sort the received data portion. 
		MergeSort(data, send_count[id], sizeof(DATA_TYPE), compfunc);
		MPI_Barrier(MPI_COMM_WORLD);		//Wait for everyone to complete sorting their sublists

		
		//Beginning merging sublists across nodes (phase 2)
		arrayCopy(merge_status, send_count, p);
		
		instr.completed = 0;
		//i = 0;
		
		while(!instr.completed)
		{
			instr = planMerge(merge_status, id, p);
			//i++;
			
			if(instr.op == SEND)
			{
				//printf("NODE: %d Iter: %d OP: SEND, Target: %d Before: %d Elements to S/R: %d After: %d Comp: %d\n", id, i-1, instr.target, send_count[id], instr.elements, merge_status[id], instr.completed);
				MPI_Send(data, send_count[id], MPI_DATA_TYPE, instr.target, 0, MPI_COMM_WORLD);
				free(data);
			}
			else if(instr.op == RECV)
			{
				//printf("NODE: %d Iter: %d OP: RECV, Target: %d Before: %d Elements to S/R: %d After: %d Comp: %d\n", id, i-1, instr.target, send_count[id], instr.elements, merge_status[id], instr.completed);
				
				data = realloc(data, sizeof(DATA_TYPE)*(send_count[id] + instr.elements));
				//printf("Node: %d, RECV buffer size: %d\n", id, send_count[id] + instr.elements);
				
				if(data == NULL)
				{
					printf("Node %d: Realloc failed! Wanted %d elements!\n", id, instr.elements);
					return -1;
				}
				
				MPI_Recv(&data[send_count[id]], instr.elements, MPI_DATA_TYPE, instr.target, 0, MPI_COMM_WORLD, &recv_status);
				
				MergeSort(data, (send_count[id] + instr.elements), sizeof(DATA_TYPE), compfunc);	//Replace with MERGE instead!
			}
			
			arrayCopy(send_count, merge_status, p);
			
			//Wait for every node to complete this iteration 
			MPI_Barrier(MPI_COMM_WORLD);
		}
	}
	
	free(send_count);
	free(merge_status);
	
	MPI_Finalize();
	return 0;
}