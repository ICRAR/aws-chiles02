#!/bin/bash -vx
yum -y update

# Print into the logs the disk free
df -h

cd /home/ec2-user
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && git pull'
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && python setup.py install'
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && pip install /home/ec2-user/aws-chiles02/pipeline/pip/requirements.txt'
runuser -l ec2-user -c 'cd /home/ec2-user && git clone https://github.com/ICRAR/aws-chiles02.git'

cat /home/ec2-user/.ssh/id_dfms.pub >> /home/ec2-user/.ssh/authorized_keys
