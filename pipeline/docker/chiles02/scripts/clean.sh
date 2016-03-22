#!/bin/bash -xv
# clean

cd /opt/chiles02/aws-chiles02
git pull

# make the directory and ensure anyone can write to it
mkdir -p $1
chmod oug+rwx $1
cd $1

# outdir, min_freq, max_freq
casapy --nologger --log2term -c /opt/chiles02/aws-chiles02/pipeline/casapy_code/clean.py $@
