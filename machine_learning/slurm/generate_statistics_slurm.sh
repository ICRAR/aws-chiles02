#!/bin/bash --login
# SLURM directives
#
#SBATCH --job-name=generate_statistics
#SBATCH --account=pawsey0216
#SBATCH --time=06:00:00
#SBATCH --ntasks=24
#SBATCH --ntasks-per-node=24
#SBATCH --nodes=2
#SBATCH --array=0-127

# with the node=2 this will run 16 versions of test_array across two nodes (8 on each)
aprun -n 16 -N 16 $SLURM_SUBMIT_DIR/generate_statistics.sh
