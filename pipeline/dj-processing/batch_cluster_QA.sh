#!/bin/bash

#PBS -q comm_mmem_day
#PBS -N batch_test
#PBS -m ae
#PBS -M djpisano@mail.wvu.edu
#PBS -l nodes=1:ppn=9,pvmem=6g
#PBS -n 
#PBS -e test.error
#PBS -o test.output
#PBS -l prologue=/users/djpisano/prologue.sh
#PBS -l epilogue=/users/djpisano/epilogue.sh

# Listing of sessions for testing: 1,2,5,8,9,10,16,17

#PBS -t 1,2,5,8,9,10,16,17

module load astronomy/casa-5.3

/usr/bin/time -v mpicasa -n 9 /shared/software/astro/casa-release-5.3.0-143.el6/bin/casa --nogui --nologger --agg -c "var=[${PBS_ARRAYID}];execfile('/users/djpisano/cluster_pipeline/CHILES_pipeline_QA_batch.py')"
