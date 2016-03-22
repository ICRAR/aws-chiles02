#!/bin/bash -xv
# mstransform

cd /opt/chiles02/aws-chiles02
git pull

# make the directory and ensure anyone can write to it
mkdir -p $2
chmod oug+rwx $2
cd $2

# infile, outdir, min_freq, max_freq
casapy --nologger --log2term -c /opt/chiles02/aws-chiles02/pipeline/casapy_code/mstransform.py $1 $2 $4 $5 $6 $7
