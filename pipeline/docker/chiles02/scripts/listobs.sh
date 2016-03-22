#!/bin/bash -xv
# listobs

cd /opt/chiles02/aws-chiles02
git pull

# infile, outdir, min_freq, max_freq
casapy --nologger --log2term -c /opt/chiles02/aws-chiles02/pipeline/casapy/listobs.py $1 $2
