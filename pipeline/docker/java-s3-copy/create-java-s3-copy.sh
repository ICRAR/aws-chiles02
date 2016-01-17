#!/bin/bash

cp ../../../java/build/awsChiles02.jar chiles02
docker build --tag java-s3-copy:latest .
