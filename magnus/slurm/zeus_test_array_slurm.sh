#!/bin/bash --login
# SLURM directives
#
#SBATCH --job-name=test_array
#SBATCH --account=pawsey0216
#SBATCH --time=00:00:10
#SBATCH --array=0-4
#SBATCH --nodes=1
#SBATCH --partition=workq
#SBATCH --cluster=zeus
#SBATCH --ntasks=1
#SBATCH --export=NONE

# with the node=1 this will run 4 versions of test_array across the node

for i in {0..3}
do
    TASK_ID=`printf %d $((SLURM_ARRAY_TASK_ID * 4 + $i))`
    test_array.sh $TASK_ID &
done

wait

