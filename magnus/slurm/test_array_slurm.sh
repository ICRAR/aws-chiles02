#!/bin/bash --login
# SLURM directives
#
#SBATCH --job-name=test_array
#SBATCH --account=pawsey0216
#SBATCH --time=00:00:10
#SBATCH --nodes=1
#SBATCH --array=0-4

# with the node=1 this will run 8 versions of test_array across the node
aprun -n 8 -N 8 $SLURM_SUBMIT_DIR/test_array.sh
