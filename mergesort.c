#include <stdio.h>
#include <stdlib.h>

#define FILENAME 	"data.txt"			//Filename of the target data file

typedef double DATA_TYPE;					//Change the data type for the values held in the database here

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

// Function to Merge Arrays L and R into A. 
// lefCount = number of elements in L
// rightCount = number of elements in R. 

void Merge(DATA_TYPE *A, DATA_TYPE *L, int leftCount, DATA_TYPE *R, int rightCount) 
{
	int i,j,k;

	// i - to mark the index of left aubarray (L)
	// j - to mark the index of right sub-raay (R)
	// k - to mark the index of merged subarray (A)
	i = 0; j = 0; k =0;

	while(i<leftCount && j< rightCount) {
		if(L[i]  < R[j]) 
			A[k++] = L[i++];
		else 
			A[k++] = R[j++];
	}

	while(i < leftCount) 
		A[k++] = L[i++];
	
	while(j < rightCount) 
		A[k++] = R[j++];
}

// Recursive function to sort an array of integers. 
void MergeSort(DATA_TYPE *A, int n) 
{
	int mid, i;
	DATA_TYPE *L, *R;
	
	if(n < 2) 
		return; // base condition. If the array has less than two element, do nothing. 

	mid = n/2;  // find the mid index. 

	
	//create left and right subarrays
	//mid elements (from index 0 till mid-1) should be part of left sub-array 
	//and (n-mid) elements (from mid to n-1) will be part of right sub-array
	L = (DATA_TYPE*) malloc (mid*sizeof(DATA_TYPE)); 
	R = (DATA_TYPE*) malloc ((n- mid)*sizeof(DATA_TYPE)); 
	
	//creating left subarray
	for(i=0; i<mid; i++) 
		L[i] = A[i]; 
	
	//creating right subarray
	for(i = mid;i<n;i++) 
		R[i-mid] = A[i]; 
	
	MergeSort(L,mid);  			//sorting the left subarray
	MergeSort(R,n-mid);  		//sorting the right subarray
	Merge(A,L,mid,R,n-mid);  	//Merging L and R into A as sorted list.
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
		printf("%f\n", data[i]);*/
		
	// Calling merge sort to sort the array. 
	MergeSort(data, elements);

	//printing all elements in the array once its sorted.
	for(i = 0; i<elements; i++) 
		printf("%f\n", data[i]);
	
	free(data);
	return 0;
}