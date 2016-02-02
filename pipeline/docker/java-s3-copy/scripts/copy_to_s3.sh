#!/bin/bash -xv
# copy_to_s3

cd /opt/chiles02/aws-chiles02
git pull

# infile, outdir, min_freq, max_freq
python /opt/chiles02/aws-chiles02/pipeline/aws_chiles02/copy_to_s3 $@
