/*Main entry point to test mergesort*/
#include "mergesort.h"

#define FILENAME 	"data.txt"			//Filename of the target data file

typedef double DATA_TYPE;				//Change the data type for the values held in the database here

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

int main() {
	
	int elements;
	int filesize;
	int i, retval;
	
	FILE *infile;
	DATA_TYPE *data; 
	
	//Open up the data file
	infile = fopen(FILENAME, "r");
	if(infile == NULL)
	{
		perror("Failed to open data file");
		return -1;
	}
	
	filesize = getFileSize(infile);
	
	/*Get the size of the input file, and allocate the data buffer based on this size.
	The resulting buffer will be a bit bigger than needed, but that's okay for now.*/
	data = (DATA_TYPE*) malloc(filesize);
	
	/*Read the data file into the buffer until we hit end of the file*/
	for(elements = 0, retval = 1; retval > 0; elements++)
		retval = fscanf(infile, "%lf\n", &data[elements]);		//NOTE: Change the format identifier here if DATA_TYPE has changed!
	
	/*Trim the unused memory out of the data buffer.
	The last element is also removed since the previous file reading method adds an extra zero element*/
	elements -= 1;
	data = realloc(data, sizeof(DATA_TYPE)*elements);
	
	printf("Read in %d elements from the data file %s\n", elements, FILENAME);
	
	/*for(i = 0; i < elements; i++)
		printf("%f\n", data[i]);
	
	printf("\n");*/
		
	// Calling merge sort to sort the array. 
	MergeSort(data, elements, sizeof(DATA_TYPE), compfunc);

	//printing all elements in the array once its sorted.
	for(i = 0; i<elements; i++) 
		printf("%f\n", data[i]);
	
	free(data);
	return 0;
}