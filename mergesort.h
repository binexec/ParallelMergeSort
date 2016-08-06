#ifndef MERGESORT_H
#define MERGESORT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Sort an array of any data type using recursive MergeSort.
	
	data 		= pointer to the first element of an array to be sorted
	n 			= number of elements in the data array
	data_size 	= size of the data's type in bytes
	comp 		= pointer to a function that compares two data items. 
				  The function must return -1 if a < b; 0 if a = b; 1 if a > b
*/
void MergeSort(void *data, int n, size_t data_size, int (*comp)(void *, void *));

/*	Function to Merge Arrays L and R into A. 

	left_n = number of elements in L
	right_n = number of elements in R. 
*/
void Merge(void *data, void *L, int left_n, void *R, int right_n, size_t data_size, int (*comp)(void *, void *));

#endif