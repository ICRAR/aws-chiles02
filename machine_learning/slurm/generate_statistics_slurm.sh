#!/bin/bash --login
# SLURM directives
#
#SBATCH --job-name=generate_statistics
#SBATCH --account=pawsey0216
#SBATCH --time=06:00:00
#SBATCH --nodes=1
#SBATCH --array=0-127

# with the node=1 this will run 4 versions of generate_statistics across the node
aprun -n 4 -N 4 $SLURM_SUBMIT_DIR/generate_statistics.sh
