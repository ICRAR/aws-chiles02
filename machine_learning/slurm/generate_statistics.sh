#!/bin/bash

# Space out the starting of the casa python on a node
SLEEP_TIME=`printf %d $((30 * ALPS_APP_PE + 10))`
sleep ${SLEEP_TIME}

# We are one of -N <NUMBER> running on a node as part of an array job,
# but the row number in queue.txt goes from 1 to X so add one as ALPS_APP_PE goes from 0
ROW_NUMBER=`printf %d $((SLURM_ARRAY_TASK_ID * 4 + ALPS_APP_PE + 1))`

# Use awk as it will return nothing if the row number does not exist
TASK_ID=`awk "NR == ${ROW_NUMBER} {print; exit}" /group/pawsey0216/kvinsen/aws-chiles02/machine_learning/casa_code/queue.txt`
IP_ADDRESS=`hostname --ip-address`

# Show the environment variables
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
ROW_NUMBER              = $ROW_NUMBER
TASK_ID                 = $TASK_ID
SLEEP_TIME              = $SLEEP_TIME
########################################"


if [ -z "{TASK_ID}" ]
then
    echo "No value for row number: ${ROW_NUMBER}"
else
    NOW=$(date +"%Y_%m_%d")
    CASA_LOG=/scratch/pawsey0216/kvinsen/casa_logs/${NOW}_${TASK_ID}.log
    cd /group/pawsey0216/kvinsen/aws-chiles02/machine_learning/casa_code
    export PYTHONPATH=/group/pawsey0216/kvinsen/aws-chiles02/machine_learning

    /group/pawsey0216/kvinsen/casa-release-4.7.2-el6/bin/casa --logfile ${CASA_LOG} --nologger --log2term --nogui -c generate_statistics.py ${TASK_ID} --magnus --settings_file_name /group/pawsey0216/kvinsen/aws-chiles02/machine_learning/casa_code/scan.settings
fi
