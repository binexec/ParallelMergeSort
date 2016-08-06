#!/bin/sh
rm error.txt
rm output.txt
rm out_*

qsub -q cluster-long -l nodes=4:ppn=8 -o output.txt -e error.txt mpirun.sh