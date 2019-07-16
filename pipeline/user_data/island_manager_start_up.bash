#!/bin/bash -vx
#
# ##    ##  #######  ######## ########
# ###   ## ##     ##    ##    ##
# ####  ## ##     ##    ##    ##
# ## ## ## ##     ##    ##    ######
# ##  #### ##     ##    ##    ##
# ##   ### ##     ##    ##    ##
# ##    ##  #######     ##    ########
#
# As this file is used by Mako don't us $\{} style substitution in bash
#
yum -y update

# Print into the logs the disk free
df -h

cd /home/ec2-user
runuser -l ec2-user -c 'cd /home/ec2-user/aws-chiles02 && git pull'
runuser -l ec2-user -c 'cd /home/ec2-user && source /home/ec2-user/virtualenv/daliuge/bin/activate && pip install --upgrade -r /home/ec2-user/aws-chiles02/pipeline/pip/requirements.txt'
runuser -l ec2-user -c 'cd /home/ec2-user && source /home/ec2-user/virtualenv/aws-chiles02/bin/activate && pip install --upgrade -r /home/ec2-user/aws-chiles02/pipeline/pip/requirements.txt'

cat /home/ec2-user/.ssh/id_daliuge.pub >> /home/ec2-user/.ssh/authorized_keys

# Do we need a node manager running
% if need_node_manager:
runuser -l ec2-user -c 'cd /home/ec2-user && source /home/ec2-user/virtualenv/daliuge/bin/activate && dlg nm --daemon -${log_level} --dlg-path=/home/ec2-user/aws-chiles02/pipeline -H 0.0.0.0 --log-dir /tmp --error-listener=aws_chiles02.error_handling.ErrorListener --max-request-size ${max_request_size}'

# Get my public IP address
METADATA_URL_BASE="http://169.254.169.254/latest"
export dim_ip=$(curl --silent $METADATA_URL_BASE/meta-data/public-ipv4)
runuser -l ec2-user -c "cd /home/ec2-user && source /home/ec2-user/virtualenv/daliuge/bin/activate && dlg dim --daemon -${log_level} -H 0.0.0.0 --ssh-pkey-path ~/.ssh/id_daliuge --nodes ${hosts},$dim_ip --log-dir /tmp --max-request-size ${max_request_size}"
% endif

% if not need_node_manager:
runuser -l ec2-user -c 'cd /home/ec2-user && source /home/ec2-user/virtualenv/daliuge/bin/activate && dlg dim --daemon -${log_level} -H 0.0.0.0 --ssh-pkey-path ~/.ssh/id_daliuge --nodes ${hosts} --log-dir /tmp --max-request-size ${max_request_size}'
% endif

sleep 10
runuser -l ec2-user -c 'cd /home/ec2-user/aws-chiles02/pipeline/aws_chiles02 && source /home/ec2-user/virtualenv/aws-chiles02/bin/activate && python startup_complete.py ${queue} ${region} "${uuid}"'
