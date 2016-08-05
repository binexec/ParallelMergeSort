#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

/*	
	Function to Merge Arrays L and R into A. 
	lefCount = number of elements in L
	rightCount = number of elements in R. 
*/

void Merge(void *data, void *L, int leftCount, void *R, int rightCount, size_t data_size, int (*comp)(void *, void *)) 
{
	int i,j,k;
	void *data1, *data2;

	// i - to mark the index of left aubarray (L)
	// j - to mark the index of right sub-raay (R)
	// k - to mark the index of merged subarray (A)
	i = 0; j = 0; k =0;

	while(i<leftCount && j< rightCount) 
	{
		data1 = L + (data_size * i);			//data1 = L[i]
		data2 = R + (data_size * j);			//data2 = R[j]
		
		//if(L[i]  < R[j]) 
		if( ((*comp)(data1, data2)) == -1)
		{
			//data[k++] = L[i++]
			data1 = data + (data_size * k++);	//data1 = &data[k++] 
			data2 = L + (data_size * i++);		//data2 = &L[i++]
		}
		else 
		{
			//data[k++] = R[j++]
			data1 = data + (data_size * k++);	//data1 = &data[k++] 
			data2 = R + (data_size * j++);		//data2 = &R[j++];
		}
		memcpy(data1, data2, data_size);
	}

	while(i < leftCount) 
	{
		//data[k++] = L[i++]
		data1 = data + (data_size * k++);		//data1 = &data[k++] 
		data2 = L + (data_size * i++);			//data2 = &L[i++]
		memcpy(data1, data2, data_size);
	}
		
	
	while(j < rightCount) 
	{
		//data[k++] = R[j++]
		data1 = data + (data_size * k++);		//data1 = &data[k++] 
		data2 = R + (data_size * j++);			//data2 = &R[j++];
		memcpy(data1, data2, data_size);
	}
		
}

/*
	Sort an array of any data type using recursive MergeSort.
	
	data 		= pointer to the data array to be sorted
	n 			= number of elements in the data array
	data_size 	= size of the data's type in bytes
	comp 		= pointer to a function that compares two data items.
					The function must return -1 if a < b; 0 if a = b; 1 if a > b
*/
void MergeSort(void *data, int n, size_t data_size, int (*comp)(void *, void *)) 
{
	int mid, i;
	void *L, *R;
	void *data1, *data2;
	
	if(n < 2) 
		return; // base condition. If the array has less than two element, do nothing. 

	mid = n/2;  // find the mid index. 

	//create left and right subarrays
	//mid elements (from index 0 till mid-1) should be part of left sub-array 
	//and (n-mid) elements (from mid to n-1) will be part of right sub-array
	L = (void *) malloc(mid * data_size); 
	R = (void *) malloc((n-mid) * data_size); 
	
	//creating left subarray
	memcpy(L, data, data_size * mid);

	//creating right subarray
	memcpy(R, (data + data_size * mid), data_size * (n-mid));
	
	//sorting the left subarray
	MergeSort(L, mid, data_size, (*comp)); 
	
	//sorting the right subarray
	MergeSort(R, n-mid, data_size, (*comp));  	

	//Merging L and R into A as sorted list.
	Merge(data, L, mid, R, n-mid, data_size, (*comp));  	
    free(L);
    free(R);
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