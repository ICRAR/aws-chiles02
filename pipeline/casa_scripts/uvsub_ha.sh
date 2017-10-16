#!/bin/bash -xv
# clean

cd /opt/chiles02/aws-chiles02
git pull

# make the directory and ensure anyone can write to it
mkdir -p $2
chmod oug+rwx $2
cd $2

# outdir, min_freq, max_freq
casa --nologger --log2term -c /opt/chiles02/aws-chiles02/pipeline/casa_code/uvsub_ha.py $@
