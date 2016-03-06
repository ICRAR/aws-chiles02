#!/bin/bash -xv
# concatenate

cd /opt/chiles02/aws-chiles02
git pull

mkdir -p $0
cd $1
# outdir, min_freq, max_freq
casapy --nologger  --log2term --logfile $0  -c /opt/chiles02/aws-chiles02/pipeline/aws_chiles02/concatenate.py $@
