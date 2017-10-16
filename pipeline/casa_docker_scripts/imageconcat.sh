#!/bin/bash -xv
# concatenate

cd /opt/chiles02/aws-chiles02
git pull

# make the directory and ensure anyone can write to it
mkdir -p $1
chmod oug+rwx $1
cd $1

# outdir,
casa --nologger --log2term -c /opt/chiles02/aws-chiles02/pipeline/casa_code/imageconcat.py $@
