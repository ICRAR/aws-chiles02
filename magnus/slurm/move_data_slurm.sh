#!/bin/bash --login
# SLURM directives
#
#SBATCH --job-name=move_data
#SBATCH --account=pawsey0216
#SBATCH --time=10:00:00
#SBATCH --ntasks=24
#SBATCH --ntasks-per-node=24
#SBATCH --nodes=1
#SBATCH --array=0-5

# with the node=1 this will run 24 versions of move_data on the same one
aprun -n 24 -N 24 $SLURM_SUBMIT_DIR/move_data.sh
