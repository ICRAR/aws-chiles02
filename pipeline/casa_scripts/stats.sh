#!/bin/bash -xv
# clean

cd /opt/chiles02/aws-chiles02
git pull

# outdir, min_freq, max_freq
casa --nologger --log2term -c /opt/chiles02/aws-chiles02/pipeline/casa_code/stats.py $@
