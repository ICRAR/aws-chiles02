#!/bin/bash -xv
# listobs

cd /opt/chiles02/aws-chiles02
git pull

# infile, outdir, min_freq, max_freq
casa --nologger --log2term -c /opt/chiles02/aws-chiles02/pipeline/casapy_code/listobs.py $1 $2
