#!/bin/bash

# We are one of 24 running on a node as part of an array job
# Show the environment variables
echo "ALPS_APP_PE = $ALPS_APP_PE"
echo "SLURM_ARRAY_TASK_ID = $SLURM_ARRAY_TASK_ID"
echo "SLURM_SUBMIT_DIR = $SLURM_SUBMIT_DIR"

task_id=`printf %d $((SLURM_ARRAY_TASK_ID * 24 + ALPS_APP_PE))`
echo "dir_id = $task_id"

cd /group/pawsey0216/kvinsen/aws-chiles02/magnus/python_src
python move_data.py 13b-266 uvsub_deep_2017_05_25 ${task_id} /group/pawsey0216/kvinsen/chiles_data
