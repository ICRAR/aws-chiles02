#!/bin/bash --login
# SLURM directives
#
#SBATCH --job-name=generate_statistics
#SBATCH --account=pawsey0216
#SBATCH --time=04:00:00
#SBATCH --ntasks=24
#SBATCH --ntasks-per-node=24
#SBATCH --nodes=1
#SBATCH --array=0-127

# with the node=1 this will run 24 versions of generate_statistics on the same one
aprun -n 24 -N 24 $SLURM_SUBMIT_DIR/generate_statistics.sh
