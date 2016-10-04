#!/usr/bin/env bash
mkdir /home/ec2-user/casa

curl -SL https://svn.cv.nrao.edu/casa/linux_distro/release/el6/casa-release-4.6.0-el6.tar.gz | tar -xzC /home/ec2-user/casa

sudo bash -c 'echo "PATH=\$PATH:/home/ec2-user/casa/casa-release-4.6.0-el6/bin" >> /etc/profile.d/casa.sh'
sudo bash -c 'echo "PYTHONPATH=\$PYTHONPATH:/home/ec2-user/aws-chiles02/pipeline" >> /etc/profile.d/casa.sh'
sudo yum install -y fontconfig freetype libpng12 libSM libXcursor libXi libXinerama libXrandr libXfixes libXrender libxslt which xauth xorg-x11-server-Xvfb
