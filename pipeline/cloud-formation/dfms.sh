#!/usr/bin/env bash

sudo yum -y update
sudo yum -y install git
git clone https://github.com/SKA-ScienceDataProcessor/dfms

sudo yum -y install gcc gcc-c++ boost-python boost-system boost-devel

virtualenv ~/virtualenv/aws-chiles02
source ~/virtualenv/aws-chiles02/bin/activate
pip install -U pip
pip install boto3 argparse ConfigObj mako

virtualenv ~/virtualenv/dfms
source ~/virtualenv/dfms/bin/activate

cd dfms
python setup.py install
pip install boto3 argparse ConfigObj mako

sudo tee -a /etc/security/limits.conf << 'EOF'

# Added to make clean work
*         hard    nofile      500000
*         soft    nofile      500000
root      hard    nofile      500000
root      soft    nofile      500000
EOF

sudo yum install -y docker
sudo service docker start
sudo usermod -a -G docker ec2-user

cd ~/.ssh
ssh-keygen -t rsa -f id_dfms -P ""
chmod og-r id_dfms.pub
