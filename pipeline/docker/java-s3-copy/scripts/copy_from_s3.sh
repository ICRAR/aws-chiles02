#!/bin/bash -xv
# copy_from_s3

java -classpath /chiles02/awsChiles02.jar org.icrar.awsChiles02.copyS3.CopyFileFromS3SingleThreaded -aws_access_key_id $3 -aws_secret_access_key $4 $1 $2.tar

# Create the directory to hold the measurement set
mkdir -p $2
tar -xvf $2.tar -C $2
rm $2.tar
