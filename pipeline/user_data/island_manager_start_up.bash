#!/bin/bash -vx
yum -y update

# Print into the logs the disk free
df -h

cd /home/ec2-user
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && git pull'
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && python setup.py install'
runuser -l ec2-user -c 'cd /home/ec2-user && git clone https://github.com/ICRAR/aws-chiles02.git'
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && pip install --upgrade -r /home/ec2-user/aws-chiles02/pipeline/pip/requirements.txt'

cat /home/ec2-user/.ssh/id_dfms.pub >> /home/ec2-user/.ssh/authorized_keys

# Do we need a node manager running
% if need_node_manager:
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && dfmsNM --daemon -${log_level} --dfms-path=/home/ec2-user/aws-chiles02/pipeline -H 0.0.0.0 --log-dir /tmp --error-listener=aws_chiles02.error_handling.ErrorListener --max-request-size ${max_request_size}'

# Get my public IP address
METADATA_URL_BASE="http://169.254.169.254/latest"
export dim_ip=$(curl --silent $METADATA_URL_BASE/meta-data/public-ipv4)
runuser -l ec2-user -c "cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && dfmsDIM --daemon -${log_level} -H 0.0.0.0 --ssh-pkey-path ~/.ssh/id_dfms --nodes ${hosts},$dim_ip --log-dir /tmp --max-request-size ${max_request_size}"
% endif

% if not need_node_manager:
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && dfmsDIM --daemon -${log_level} -H 0.0.0.0 --ssh-pkey-path ~/.ssh/id_dfms --nodes ${hosts} --log-dir /tmp --max-request-size ${max_request_size}'
% endif

sleep 10
runuser -l ec2-user -c 'cd /home/ec2-user/aws-chiles02/pipeline/aws_chiles02 && source /home/ec2-user/virtualenv/aws-chiles02/bin/activate && python startup_complete.py ${queue} ${region} "${uuid}"'
