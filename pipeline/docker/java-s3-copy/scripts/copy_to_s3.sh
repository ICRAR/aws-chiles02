#!/bin/bash -xv
# copy_to_s3

cd $1

if [ -d vis_$5~$6 ]; then
    tar -cvf vis.tar vis_$5~$6

    java -classpath /chiles02/awsChiles02.jar org.icrar.awsChiles02.copyS3.CopyFileToS3 -aws_access_key_id $3 -aws_secret_access_key $4 $2 vis.tar
    rm vis.tar
fi
