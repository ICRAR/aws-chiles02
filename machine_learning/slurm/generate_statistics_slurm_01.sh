#!/bin/bash --login
# SLURM directives
#
#SBATCH --job-name=generate_statistics
#SBATCH --account=pawsey0216
#SBATCH --time=06:00:00
#SBATCH --nodes=1
#SBATCH --array=3020,3022,3023,3048,3051,3056,3058,3059

# with the node=1 this will run 1 versions of generate_statistics across the node
aprun -n 1 -N 1 $SLURM_SUBMIT_DIR/generate_statistics.sh
