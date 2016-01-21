#!/bin/bash -xv
# copy_from_s3

java -classpath /chiles02/awsChiles02.jar org.icrar.awsChiles02.copyS3.CopyFileFromS3SingleThreaded -aws_access_key_id $3 -aws_secret_access_key $4 $1 $2.tar
#tar -xvf $2.tar $2
#rm $2.tar
