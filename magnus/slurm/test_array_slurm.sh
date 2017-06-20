#!/bin/bash --login
# SLURM directives
#
#SBATCH --job-name=test_array
#SBATCH --account=pawsey0216
#SBATCH --time=00:00:10
#SBATCH --nodes=1
#SBATCH --array=0-4

# with the node=1 this will run 24 versions of generate_statistics on the same one
aprun -n 12 -N 12 $SLURM_SUBMIT_DIR/test_array.sh
