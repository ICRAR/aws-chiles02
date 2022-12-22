#!/bin/bash
if [ $# -eq 0 ]
  then
    echo "No arguments supplied."
fi

echo "Arg: $1"

module load rclone/1.59.2

rclone copy acacia-dingo:chiles01/$1 /scratch/pawsey0411/chiles/$1 --progress --size-only --s3-disable-checksum --s3-chunk-size 128M