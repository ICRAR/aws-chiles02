#!/bin/bash

# We are one of 24 running on a node as part of an array job
# Show the environment variables
echo "ALPS_APP_PE             = $ALPS_APP_PE"
echo "SLURM_SUBMIT_DIR 	      = $SLURM_SUBMIT_DIR"
echo "SLURM_JOB_NAME 	      = $SLURM_JOB_NAME"
echo "SLURM_JOB_ID 	          = $SLURM_JOB_ID"
echo "SLURM_JOB_NODELIST 	  = $SLURM_JOB_NODELIST"
echo "SLURM_NTASKS 	          = $SLURM_NTASKS"
echo "SLURM_JOB_CPUS_PER_NODE = $SLURM_JOB_CPUS_PER_NODE"
echo "SLURM_JOB_NUM_NODES 	  = $SLURM_JOB_NUM_NODES"
echo "SLURM_ARRAY_TASK_ID 	  = $SLURM_ARRAY_TASK_ID"
echo "SLURM_ARRAY_JOB_ID      = $SLURM_ARRAY_JOB_ID"

task_id=`printf %d $((SLURM_ARRAY_TASK_ID * 24 + ALPS_APP_PE))`
echo "task_id = $task_id"
