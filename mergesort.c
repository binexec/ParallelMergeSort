#include "mergesort.h"

/*	Function to Merge Arrays L and R into A. 

	left_n = number of elements in L
	right_n = number of elements in R. 
*/
void Merge(void *data, void *L, int left_n, void *R, int right_n, size_t data_size, int (*comp)(void *, void *)) 
{
	int i = 0;				//Index of left aubarray (L)
	int j = 0;				//Index of right sub-raay (R)
	int k = 0;				//Index of merged subarray (A)
	
	void *data1, *data2;	//Temporary pointers

	while(i < left_n && j < right_n) 
	{
		data1 = L + (data_size * i);			//data1 = &L[i]
		data2 = R + (data_size * j);			//data2 = &R[j]
		
		if( ((*comp)(data1, data2)) == -1)		//if(L[i]  < R[j]) 
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

	while(i < left_n) 
	{
		data1 = data + (data_size * k++);		//data1 = &data[k++] 
		data2 = L + (data_size * i++);			//data2 = &L[i++]
		memcpy(data1, data2, data_size);		//data[k++] = L[i++]
	}
		
	while(j < right_n) 
	{
		data1 = data + (data_size * k++);		//data1 = &data[k++] 
		data2 = R + (data_size * j++);			//data2 = &R[j++];
		memcpy(data1, data2, data_size);		//data[k++] = R[j++]
	}	
}

/* Sort an array of any data type using recursive MergeSort.
	
	data 		= pointer to the first element of an array to be sorted
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
		return; 

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