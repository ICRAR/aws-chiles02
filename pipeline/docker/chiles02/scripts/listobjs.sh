#!/bin/bash -xv
# listobjs

cd /opt/chiles02/aws-chiles02
git pull

mkdir -p $2
cd $2
# infile, outdir, min_freq, max_freq
casapy --nologger  --log2term --logfile $2  -c /opt/chiles02/aws-chiles02/pipeline/aws_chiles02/listobjs.py $1 $3
