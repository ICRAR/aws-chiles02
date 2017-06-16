#!/bin/bash -vx

sudo yum groupinstall "Development Tools"
sudo yum install fontconfig freetype libpng12 libSM libXcursor libXi libXinerama libXrandr libXfixes libXrender libxslt perl which xauth xorg-x11-server-Xvfb
wget https://casa.nrao.edu/download/distro/linux/release/el6/casa-release-4.7.2-el6.tar.gz
tar -xvf casa-release-4.7.2-el6.tar.gz
rm casa-release-4.7.2-el6.tar.gz
~/casa-release-4.7.2-el6/bin/casa--nologger --log2term
curl --silent --show-error --retry 5 https://bootstrap.pypa.io/get-pip.py | ~/casa-release-4.7.2-el6/bin/python
~/casa-release-4.7.2-el6/bin/pip install astroplan astropy boto3 configobj sqlalchemy


echo "Create the ~/.aws/credentials file"
echo "Start casa and run
from astroplan import download_IERS_A
download_IERS_A()"
echo "Add export PYTHONPATH=/home/ec2-user/aws-chiles02/machine_learning to ~/.bashrc"
