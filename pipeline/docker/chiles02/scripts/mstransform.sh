#!/bin/bash -xv
# mstransform

cd $2
# infile, outdir, min_freq, max_freq, step_freq, width_freq, sel_freq, spec_window)
casapy --nologger  --log2term --logfile $3  -c /opt/chiles02/aws-chiles02/pipeline/aws_chile02/mstransform.py $1 $2 $4 $5 4 15.625 1 ''
