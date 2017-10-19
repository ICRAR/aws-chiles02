#!/usr/bin/env bash

sudo yum -y update
sudo yum -y install git

sudo yum -y install gcc gcc-c++ boost-python boost-system boost-devel

sudo yum install -y fontconfig freetype libpng12 libSM libXcursor libXi libXinerama libXrandr libXfixes libXrender libxslt libXft which xauth xorg-x11-server-Xvfb

curl -SL https://casa.nrao.edu/download/distro/linux/release/el6/casa-release-5.1.0-74.el6.tar.gz | tar -xzC /home/ec2-user/

ln -s casa-release-5.1.0-74.el6/ casa

sudo bash -c 'echo "PATH=\$PATH:/home/ec2-user/casa/bin" >> /etc/profile.d/casa.sh'
sudo bash -c 'print "Appending CHILES script directory to the PYTHONPATH."' > ~/.casa/init.py
sudo bash -c 'sys.path.append("/home/ec2-user/aws-chiles02/pipeline")' >> ~/.casa/init.py

virtualenv ~/virtualenv/aws-chiles02
source ~/virtualenv/aws-chiles02/bin/activate
pip install -U pip
pip install boto3 argparse ConfigObj mako

virtualenv ~/virtualenv/daliuge
source ~/virtualenv/daliuge/bin/activate

cd daliuge
python setup.py install
pip install boto3 argparse ConfigObj mako numpy

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
ssh-keygen -t rsa -f id_daliuge -P ""
chmod og-r id_daliuge.pub
