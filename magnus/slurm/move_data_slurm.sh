#!/bin/bash --login
# SLURM directives
#
#SBATCH --job-name=move_data
#SBATCH --account=pawsey0216
#SBATCH --time=10:00:00
#SBATCH --nodes=1
#SBATCH --partition=copyq
#SBATCH --cluster=zeus
#SBATCH --ntasks=1
#SBATCH --export=NONE

source /group/pawsey0216/kvinsen/anaconda2/envs/aws-chiles02/bin/activate aws-chiles02
cd /group/pawsey0216/kvinsen/aws-chiles02/magnus/python_src
cat list_keys.txt | xargs -I KEY --max-procs=8 bash -c python move_data.py 13b-266 KEY /group/pawsey0216/kvinsen/chiles_data
