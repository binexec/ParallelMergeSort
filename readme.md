# Parallel Mergesort for HPC Applications

This experiment tests the scalability of the traditional mergesort algorithm when parallelized across several NUMA nodes, by measuring the performance of sorting 10,000,000 randomly generated double-precision floating point numbers. The performance was measured on a cluster of 4 nodes, where each nodes has 8GB of memory and 2 Intel Xeon E5520 CPUs running at 2.27GHz, with each CPU having 4 physical cores and Hyper-Threading disabled. 

The parallelized mergesort algorithm is also generalized to sort any arbritrary data structured specified by the user, as long as the user provides a proper comparison function. Written in C with OpenMPI, the algorithm should theroretically run on any UMA and NUMA systems. 


Please refer to _Parallel Merge Sort.pdf_ for full documentation of the algorithm, performance analysis, and usage example.
