#!/bin/bash -xv
# copy_from_s3

s3_name=$(basename "$1")
measurement_set="${s3_name%.*}"
if [ -d $2/${measurement_set} ] ; then
  # We already have the file
  exit 0
fi

java -classpath /chiles02/awsChiles02.jar org.icrar.awsChiles02.copyS3.CopyFileFromS3 -aws_access_key_id $3 -aws_secret_access_key $4 $1 $2.tar

if [ $? == 0 ] && [ -f $2.tar ] ; then
    # Create the directory to hold the measurement set
    mkdir -p $2
    tar -xvf $2.tar -C $2
    rm $2.tar
    exit $?
fi

exit 1
