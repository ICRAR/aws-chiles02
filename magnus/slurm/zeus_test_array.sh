#!/bin/bash

# We are one of -N <NUMBER> running on a node as part of an array job
# Show the environment variables
IP_ADDRESS=`hostname --ip-address`
echo "SLURM_SUBMIT_DIR        = $SLURM_SUBMIT_DIR
SLURM_JOB_NAME          = $SLURM_JOB_NAME
SLURM_JOB_ID            = $SLURM_JOB_ID
SLURM_JOB_NODELIST      = $SLURM_JOB_NODELIST
SLURM_NTASKS            = $SLURM_NTASKS
SLURM_JOB_CPUS_PER_NODE = $SLURM_JOB_CPUS_PER_NODE
SLURM_JOB_NUM_NODES     = $SLURM_JOB_NUM_NODES
SLURM_ARRAY_TASK_ID     = $SLURM_ARRAY_TASK_ID
SLURM_ARRAY_JOB_ID      = $SLURM_ARRAY_JOB_ID

IP_ADDRESS              = $IP_ADDRESS
TASK_ID                 = $1
########################################"

ps
