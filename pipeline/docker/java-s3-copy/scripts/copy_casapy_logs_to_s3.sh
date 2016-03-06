#!/bin/bash -xv
# copy_casapy_logs_to_s3

cd /opt/chiles02/aws-chiles02
git pull

# infile
python /opt/chiles02/aws-chiles02/pipeline/aws_chiles02/copy_casapy_logs_to_s3.py $@
