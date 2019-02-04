#!/usr/bin/env bash

aws s3 sync chiles/ s3://13b-266/observation_data/phase_1 --storage-class REDUCED_REDUNDANCY --exclude '*' --include '*.gz' --profile 'aws-chiles02'
