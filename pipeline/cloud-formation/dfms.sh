#!/usr/bin/env bash

sudo yum -y update
sudo yum -y install git
git clone https://github.com/SKA-ScienceDataProcessor/dfms

sudo yum -y install gcc gcc-c++ boost-python boost-system boost-devel

virtualenv ~/virtualenv/dfms
source ~/virtualenv/dfms/bin/activate

cd dfms
python setup.py install
