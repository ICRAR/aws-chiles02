#!/bin/bash -xv
# mstransform

cd /opt/chiles02/aws-chiles02
git pull

mkdir -p $2
chmod oug+rwx $2
cd $2
# infile, outdir, min_freq, max_freq
casapy --nologger  --log2term --logfile $3  -c /opt/chiles02/aws-chiles02/pipeline/aws_chiles02/mstransform.py $1 $2 $4 $5 $6
