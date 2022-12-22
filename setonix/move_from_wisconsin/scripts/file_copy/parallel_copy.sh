#!/bin/bash
echo "Arg: $1"

#cat $1 | xargs -n 2 -t -P 3 rsync --dry-run --size-only --partial --partial-dir=/scratch/pawsey0411/kvinsen/rsync_partials -rlvh -e "ssh -i ~/.ssh/pawsey"
xargs -0 -a $1 -P 2 rclone/rclone sync --progress -vv --size-only --s3-disable-checksum --s3-upload-concurrency 8 --s3-chunk-size 128M