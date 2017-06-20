#!/bin/bash

# We are one of 24 running on a node as part of an array job
# Show the environment variables
echo "ALPS_APP_PE = $ALPS_APP_PE"
echo "SLURM_ARRAY_TASK_ID = $SLURM_ARRAY_TASK_ID"
echo "SLURM_SUBMIT_DIR = $SLURM_SUBMIT_DIR"

task_id=`printf %d $((SLURM_ARRAY_TASK_ID * 24 + ALPS_APP_PE))`
echo "dir_id = $task_id"

cd /group/pawsey0216/kvinsen/aws-chiles02/machine_learning/casa_code
export PYTHONPATH=/group/pawsey0216/kvinsen/aws-chiles02/machine_learning
/group/pawsey0216/kvinsen/casa-release-4.7.2-el6/bin/casa --nologger --log2term -c generate_statistics.py 13b-266 uvsub_deep_2017_05_25 ${task_id} --magnus --settings_file_name /group/pawsey0216/kvinsen/aws-chiles02/machine_learning/casa_code/scan.settings
