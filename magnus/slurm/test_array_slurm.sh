#!/bin/bash --login
# SLURM directives
#
#SBATCH --job-name=test_array
#SBATCH --account=pawsey0216
#SBATCH --time=00:00:10
#SBATCH --nodes=2
#SBATCH --array=0-4

# with the node=2 this will run 16 versions of test_array across two nodes (8 on each)
aprun -n 16 -N 16 $SLURM_SUBMIT_DIR/test_array.sh
