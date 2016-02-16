#!/bin/bash -vx
yum -y update

# Print into the logs the disk free
df -h

cd /home/ec2-user
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && git pull'
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && python setup.py install'
runuser -l ec2-user -c 'cd /home/ec2-user && git clone https://github.com/ICRAR/aws-chiles02.git'

cat /home/ec2-user/.ssh/id_dfms.pub >> /home/ec2-user/.ssh/authorized_keys
runuser -l ec2-user -c 'cd /home/ec2-user/aws-chiles02/pipeline/aws_chiles02 && source /home/ec2-user/virtualenv/aws-chiles02/bin/activate && python startup_complete.py startup_complete us-west-2'
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && dfmsDIM --rest -v --id=kv -H 0.0.0.0 --ssh-pkey-path ~/.ssh/id_dfms --nodes {0} > /tmp/logfile.log 2>&1 &'
