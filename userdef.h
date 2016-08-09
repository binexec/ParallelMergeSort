/*
The userdef.h file provides an interface of all the function a user must implement in order to perform merge sort on a custom set of data.
The implemented function must be contained in "userdef.c". 
*/

#ifndef PSORT_USERDEF
#define PSORT_USERDEF

#define DATA_TYPE double				//What is the C data type your data set will be using?
#define MPI_DATA_TYPE MPI_DOUBLE		//What is the MPI data type your data set will be using? This must be equivalent to above.

/*
The userInt function allows the user to run any initialization code the user requires. 
This function is called right after MPI initializes, therefore the user must be careful 
if the code is intended to run on just the master, or all processes.
If the user needs to define a custom MPI data type to match the corresponding C data type, 
the creation can be done here.
*/
void userInit();

/*
compFunc is a comparison function that takes two elements of the user's specified C data type, and compares the two.
The function must return -1 if a < b, return 0 if a = b, and return 1 if a > b
*/
int compFunc(DATA_TYPE *a, DATA_TYPE *b);

/*
The readDataFromFile function reads an unsorted data file containing the user's data into a buffer, and returns a pointer to the buffer. 
This buffer must be allocated inside this function. 
The location and name of the data file is specified by the input parameter "filename", 
and "elements_read" is an OUTPUT parameter used to return how many elements were read in the data file. 
*/
DATA_TYPE* readDataFromFile(char *filename, int *elements_read);

/*
The writeDataToFile function is the opposite of readDataFromFile. It's intended to write the sorted data back into a file.
Once the user's data has been sorted, it's passed into this function as "data", and "n" specifies how many elements are in the data buffer. 
The "filename" paramter is the name of the output file. 
*/
void writeDataToFile(DATA_TYPE *data, int n, char *filename);

#endif