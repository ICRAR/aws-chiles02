#!/bin/bash

# Space out the starting of the python
SLEEP_TIME=`printf %d $((30 * ALPS_APP_PE + 10))`
sleep ${SLEEP_TIME}

# We are one of -N <NUMBER> running on a node as part of an array job
# Show the environment variables
TASK_ID=`printf %d $((SLURM_ARRAY_TASK_ID * 4 + ALPS_APP_PE))`
IP_ADDRESS=`hostname --ip-address`
echo "ALPS_APP_PE             = $ALPS_APP_PE
SLURM_SUBMIT_DIR        = $SLURM_SUBMIT_DIR
SLURM_JOB_NAME          = $SLURM_JOB_NAME
SLURM_JOB_ID            = $SLURM_JOB_ID
SLURM_JOB_NODELIST      = $SLURM_JOB_NODELIST
SLURM_NTASKS            = $SLURM_NTASKS
SLURM_JOB_CPUS_PER_NODE = $SLURM_JOB_CPUS_PER_NODE
SLURM_JOB_NUM_NODES     = $SLURM_JOB_NUM_NODES
SLURM_ARRAY_TASK_ID     = $SLURM_ARRAY_TASK_ID
SLURM_ARRAY_JOB_ID      = $SLURM_ARRAY_JOB_ID

IP_ADDRESS              = $IP_ADDRESS
TASK_ID                 = $TASK_ID
SLEEP_TIME              = $SLEEP_TIME
########################################"

cd /group/pawsey0216/kvinsen/aws-chiles02/machine_learning/casa_code
export PYTHONPATH=/group/pawsey0216/kvinsen/aws-chiles02/machine_learning

/group/pawsey0216/kvinsen/casa-release-4.7.2-el6/bin/casa --nologfile --nologger --log2term --nogui -c generate_statistics.py 13b-266 uvsub_deep_2017_05_25 ${TASK_ID} --magnus --settings_file_name /group/pawsey0216/kvinsen/aws-chiles02/machine_learning/casa_code/scan.settings
