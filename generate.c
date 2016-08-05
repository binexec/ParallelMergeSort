/*Generates a random data set to test sorting*/

#include <stdio.h>
#include <stdlib.h>
#include <float.h>

#define ELEMENTS 	10000000				//How many elements to generate in the target data file
#define FILENAME 	"data.txt"				//Filename of the target data file

//typedef DATA_TYPE double; 

int main()
{
	FILE *outfp;
	
	int i;
	double newval, scale;
	
	//Seed the random number generator using current time
	srand((unsigned)time(NULL));
	
	//Open the output file for writing
	outfp = fopen(FILENAME, "w");
	
	//Start generating!
	for(i=0; i<ELEMENTS; i++)
	{
		//generate a random float
		scale = (double)rand();
		newval = ((double)rand() / (double)RAND_MAX) * scale;
		
		//Write the newly generate value on a new line
		fprintf(outfp, "%f", newval);
		
		//Add a new line character as long as it's not the last element
		if(i < ELEMENTS - 1)	
			fprintf(outfp, "\n");
	}
	
	printf("Written %d elements to the file %s\n", ELEMENTS, FILENAME);
}