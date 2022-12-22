#!/bin/bash

if [ $# -eq 0 ]
  then
    echo "No arguments supplied."
fi

module load rclone/1.59.2

rclone copy /scratch/pawsey0411/chiles/$1 acacia-dingo:chiles01/$1 --progress --size-only --s3-disable-checksum --s3-upload-concurrency 8 --s3-chunk-size 128M