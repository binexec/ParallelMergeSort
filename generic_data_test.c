#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

//Change the data type here
typedef double DATA_TYPE;

/*Sample Comparison function. Note that a comp function must take two pointer operands*/
int compfunc(DATA_TYPE *a, DATA_TYPE *b){
	
	DATA_TYPE deref_a = *a;
	DATA_TYPE deref_b = *b;
	
	printf("COMPFUNC. A:%f, B:%f\n", deref_a, deref_b);
	
	if(deref_a < deref_b) 
		return -1;
	else if(deref_a > deref_b) 
		return 1;
	else 
		return 0;
}

void testcomp(void *data, size_t data_length, size_t data_size, int (*comp)(void *, void *)) {
	int i, retval;
	void *data1, *data2; 
	
	for(i=0; i<data_length-1; i++)
	{
		//Calculate the new memory address to data[i] and data[i+1]
		data1 = data + (data_size * i);
		data2 = data + (data_size * (i+1));
		
		//Compare the two values at data[i] and data[i+1]
		retval = ((*comp)(data1, data2));
		
		//Must change the format symbols manually here for a and b!
		printf("a:%lf, b:%lf, comp:%d\n\n", *(DATA_TYPE*)data1, *(DATA_TYPE*)data2, retval);
	}
}

int main()
{
	DATA_TYPE vals[6] = {16.1438452345645634563, 16.9429465345645634563, 6123.20145634563456384563, 0.5, -1.06145634563456345, -1.06145634563456345};
	testcomp(vals, 6, sizeof(DATA_TYPE), compfunc);
}