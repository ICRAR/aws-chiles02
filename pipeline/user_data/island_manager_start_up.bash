#!/bin/bash -vx
yum -y update

# Print into the logs the disk free
df -h

cd /home/ec2-user
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && git pull'
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && python setup.py install'
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && pip install --upgrade -r /home/ec2-user/aws-chiles02/pipeline/pip/requirements.txt'
runuser -l ec2-user -c 'cd /home/ec2-user && git clone https://github.com/ICRAR/aws-chiles02.git'

cat /home/ec2-user/.ssh/id_dfms.pub >> /home/ec2-user/.ssh/authorized_keys

runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && dfmsDIM --daemon -vvv -H 0.0.0.0 --ssh-pkey-path ~/.ssh/id_dfms --nodes ${hosts} --log-dir /tmp --max-request-size ${max_request_size}'
sleep 10
runuser -l ec2-user -c 'cd /home/ec2-user/aws-chiles02/pipeline/aws_chiles02 && source /home/ec2-user/virtualenv/aws-chiles02/bin/activate && python startup_complete.py ${queue} ${region} "${uuid}"'
