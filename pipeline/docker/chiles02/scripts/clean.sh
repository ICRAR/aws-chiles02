#!/bin/bash -xv
# clean

ulimit -n 8192
ulimit -Hn
ulimit -Sn

cd /opt/chiles02/aws-chiles02
git pull

mkdir -p $1
cd $1
# outdir, min_freq, max_freq
casapy --nologger  --log2term --logfile $1  -c /opt/chiles02/aws-chiles02/pipeline/aws_chiles02/clean.py $@
