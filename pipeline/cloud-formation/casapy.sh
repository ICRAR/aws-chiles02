#!/usr/bin/env bash
mkdir /home/ec2-user/casapy

curl -SL https://svn.cv.nrao.edu/casa/linux_distro/release/el6/casa-release-4.5.2-el6.tar.gz | tar -xzC /home/ec2-user/casapy

sudo bash -c 'echo "PATH=$PATH:/home/ec2-user/casapy/casa-release-4.5.2-el6/bin" >> /etc/profile.d/casapy.sh'

sudo yum install -y fontconfig freetype libpng12 libSM libXcursor libXi libXinerama libXrandr libXfixes libXrender libxslt which xauth xorg-x11-server-Xvfb
