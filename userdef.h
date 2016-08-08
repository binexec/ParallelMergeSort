#ifndef PSORT_USERDEF
#define PSORT_USERDEF

#define DATA_TYPE double				//Change the data type for the values held in the database here
#define MPI_DATA_TYPE MPI_DOUBLE		//MPI data type must be equivalent to DATA_TYPE. A custom MPI data type might be needed.

void userInit();

int compFunc(DATA_TYPE *a, DATA_TYPE *b);

DATA_TYPE* readDataFromFile(char *filename, int *elements_read);

void writeDataToFile(DATA_TYPE *data, int n, char *filename);

#endif