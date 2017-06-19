#!/bin/bash --login
# SLURM directives
#
#SBATCH --job-name=move_data
#SBATCH --account=pawsey0216
#SBATCH --time=10:00:00
#SBATCH --nodes=1
#SBATCH --array=0-127
#SBATCH --partition=copyq
#SBATCH --cluster=zeus
#SBATCH --ntasks=1
#SBATCH --export=NONE

cd /group/pawsey0216/kvinsen/aws-chiles02/magnus/python_src
python move_data.py 13b-266 uvsub_deep_2017_05_25 ${SLURM_ARRAY_TASK_ID} /group/pawsey0216/kvinsen/chiles_data
