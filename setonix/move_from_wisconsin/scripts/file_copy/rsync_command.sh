#!/bin/bash

rsync --dry-run --update -rlvhP -e "ssh -i ~/.ssh/pawsey" /mnt/cephfs/projects/wilcots/$1 kvinsen@data-mover.pawsey.org.au:/scratch/pawsey0411/kvinsen/chiles/originals/$DATE