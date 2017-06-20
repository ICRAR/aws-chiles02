#!/bin/bash --login
# SLURM directives
#
#SBATCH --job-name=test_array
#SBATCH --account=pawsey0216
#SBATCH --time=00:00:10
#SBATCH --nodes=4
#SBATCH --array=0-4

# with the node=4 this will run 24 versions of test_array across four nodes (6 on each)
aprun -n 6 -N 6 $SLURM_SUBMIT_DIR/test_array.sh
