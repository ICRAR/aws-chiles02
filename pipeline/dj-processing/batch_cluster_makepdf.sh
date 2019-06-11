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

module load singularity/2.5.1

/usr/bin/time -v "var=[${PBS_ARRAYID}];execfile('/users/djpisano/cluster_pipeline/CHILES_pipeline_makepdf.sh')"
