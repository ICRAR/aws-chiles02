#!/bin/bash -xv
# copy_to_s3

tar -cvf $2.tar $2

java -classpath /chiles02/awsChiles02.jar org.icrar.awsChiles02.copyS3.CopyFileToS3 -aws_access_key_id $3 -aws_secret_access_key $4 $1 $2.tar
rm $2.tar
