#!/usr/bin/env bash

sudo yum install git
git clone https://github.com/SKA-ScienceDataProcessor/dfms

sudo yum install gcc gcc-c++ boost-python boost-system boost-devel

sudo python setup.py install
