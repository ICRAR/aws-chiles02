#!/bin/bash --login
# SLURM directives
#
#SBATCH --job-name=generate_statistics
#SBATCH --account=pawsey0216
#SBATCH --time=06:00:00
#SBATCH --nodes=1
#SBATCH --array=640-767

# Array=0-127          0    511
# Array=128-255		 512   1023
# Array=256-383		1024   1535
# Array=384-511		1536   2047
# Array=512-639		2048   2559
# Array=640-767		2560   3071
# Array=768-895		3072   3583
# Array=896-1023	3584   4095
# Array=1024-1151	4096   4607
# Array=1152-1279	4608   5119

# with the node=1 this will run 4 versions of generate_statistics across the node
aprun -n 4 -N 4 $SLURM_SUBMIT_DIR/generate_statistics.sh
