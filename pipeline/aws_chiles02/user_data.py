#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
"""
The user data we use
"""
import base64
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from os.path import dirname, join

from aws_chiles02.settings_file import QUEUE

LOG = logging.getLogger(__name__)


def get_file_contents(file_name):
    here = dirname(__file__)
    bash = join(here, '../user_data', file_name)
    with open(bash, 'r') as my_file:
        data = my_file.read()
    return data


def get_user_data(cloud_init_data):
    user_data = MIMEMultipart()
    for cloud_init in cloud_init_data:
        user_data.attach(MIMEText(cloud_init))

    encode = user_data.as_string().encode("ascii")
    encoded_data = base64.b64encode(encode).decode('ascii')

    return encoded_data


def get_node_manager_user_data(boto_data, uuid):
    cloud_init = get_file_contents('dfms_cloud_init.yaml')
    cloud_init_dynamic = '''#cloud-config
# Write the boto file
write_files:
  - path: "/etc/sysconfig/docker"
    permissions: "0644"
    owner: "root"
    content: |
      # The max number of open files for the daemon itself, and all
      # running containers.  The default value of 1048576 mirrors the value
      # used by the systemd service unit.
      DAEMON_MAXFILES=1048576

      # Additional startup options for the Docker daemon, for example:
      # OPTIONS="--ip-forward=true --iptables=true"
      # By default we limit the number of open files per container
      OPTIONS="-D --default-ulimit nofile=16384:16384"
  - path: "/etc/sysconfig/docker-storage-setup"
    permissions: "0544"
    owner: "root"
    content: |
      VG=dfms-group
      DATA_SIZE=100GB
  - path: "/root/.aws/credentials"
    permissions: "0544"
    owner: "root"
    content: |
      [{0}]
      aws_access_key_id = {1}
      aws_secret_access_key = {2}
  - path: "/home/ec2-user/.aws/credentials"
    permissions: "0544"
    owner: "ec2-user:ec2-user"
    content: |
      [{0}]
      aws_access_key_id = {1}
      aws_secret_access_key = {2}
'''.format(
            'aws-chiles02',
            boto_data[0],
            boto_data[1],
    )
    user_script = get_file_contents('node_manager_start_up.bash')
    dynamic_script = '''#!/bin/bash -vx
runuser -l ec2-user -c 'cd /home/ec2-user/aws-chiles02/pipeline/aws_chiles02 && source /home/ec2-user/virtualenv/aws-chiles02/bin/activate && python startup_complete.py {1} us-west-2 "{0}"'
'''.format(uuid, QUEUE)
    user_data = get_user_data([cloud_init, cloud_init_dynamic, user_script, dynamic_script])
    return user_data


def get_data_island_manager_user_data(boto_data, hosts, uuid):
    cloud_init = get_file_contents('dfms_cloud_init.yaml')
    cloud_init_dynamic = '''#cloud-config
# Write the boto file
write_files:
  - path: "/root/.aws/credentials"
    permissions: "0544"
    owner: "root"
    content: |
      [{0}]
      aws_access_key_id = {1}
      aws_secret_access_key = {2}
  - path: "/home/ec2-user/.aws/credentials"
    permissions: "0544"
    owner: "ec2-user:ec2-user"
    content: |
      [{0}]
      aws_access_key_id = {1}
      aws_secret_access_key = {2}
'''.format(
            'aws-chiles02',
            boto_data[0],
            boto_data[1],
    )
    user_script = get_file_contents('island_manager_start_up.bash')
    dynamic_script = '''#!/bin/bash -vx
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && dfmsDIM -vvv -H 0.0.0.0 --ssh-pkey-path ~/.ssh/id_dfms --nodes {0} > /tmp/logfile.log 2>&1 &'
sleep 10
runuser -l ec2-user -c 'cd /home/ec2-user/aws-chiles02/pipeline/aws_chiles02 && source /home/ec2-user/virtualenv/aws-chiles02/bin/activate && python startup_complete.py {1} us-west-2 "{2}"'
'''.format(hosts, QUEUE, uuid)
    user_data = get_user_data([cloud_init, cloud_init_dynamic, user_script, dynamic_script])
    return user_data
