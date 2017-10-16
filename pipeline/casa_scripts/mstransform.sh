#!/bin/bash -xv
# mstransform

cd /opt/chiles02/aws-chiles02
git pull

# make the directory and ensure anyone can write to it
mkdir -p $2
chmod oug+rwx $2
cd $2

# infile, outdir, min_freq, max_freq
casa --nologger --log2term -c /opt/chiles02/aws-chiles02/pipeline/casa_code/mstransform.py $@
