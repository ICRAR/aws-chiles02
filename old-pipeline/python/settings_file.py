#
#    (c) UWA, The University of Western Australia
#    M468/35 Stirling Hwy
#    Perth WA 6009
#    Australia
#
#    Copyright by UWA, 2012-2015
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
Chiles settings
"""
from os.path import exists, dirname, expanduser

from configobj import ConfigObj


class AwsInstance:
    def __init__(self, vCPU, memory, number_disks, size, iops_support):
        self.vCPU = vCPU
        self.memory = memory
        self.number_disks = number_disks
        self.size = size
        self.iops_support = iops_support


CHILES_CVEL_OUTPUT = '/mnt/output/Chiles/split_vis'
CHILES_CLEAN_OUTPUT = '/mnt/output/Chiles/split_cubes'
CHILES_IMGCONCAT_OUTPUT = '/mnt/output/Chiles'
CHILES_BUCKET_NAME = 'chiles.output.icrar.org'
CHILES_LOGS = '/home/ec2-user/Chiles/casa_work_dir'
BENCHMARKING_LOGS = '/tmp/trace_logs'

AWS_KEY = expanduser('~/.ssh/aws-chiles-sydney.pem')
PIP_PACKAGES = 'configobj boto psutil'
USERNAME = 'ec2-user'

FREQUENCY_WIDTH = 4
FREQUENCY_GROUPS = []

# for bottom_freq in range(1200, 1204, FREQUENCY_WIDTH):
for bottom_freq in range(940, 1424, FREQUENCY_WIDTH):
    FREQUENCY_GROUPS.append([bottom_freq, bottom_freq + FREQUENCY_WIDTH])

AWS_AMI_ID = None
AWS_KEY_NAME = None
AWS_SECURITY_GROUPS = None
AWS_REGION = None
AWS_SUBNETS = None

BASH_SCRIPT_CVEL = None
BASH_SCRIPT_CLEAN = None
BASH_SCRIPT_CLEAN_ALL = None
BASH_SCRIPT_MAKECUBE = None
BASH_SCRIPT_SETUP_DISKS = 'setup_disks.bash'

# AwsInstance(vCPU, Mem, Num Disks, Size, IOPS support)
AWS_INSTANCES = {
    'm3.medium': AwsInstance(1, 3.75, 1, 4, False),
    'm3.large': AwsInstance(2, 7.5, 1, 32, False),
    'm3.xlarge': AwsInstance(4, 15, 2, 40, True),
    'm3.2xlarge': AwsInstance(8, 30, 2, 80, True),
    'c3.large': AwsInstance(2, 3.75, 2, 16, False),
    'c3.xlarge': AwsInstance(4, 7.5, 2, 40, True),
    'c3.2xlarge': AwsInstance(8, 15, 2, 80, True),
    'c3.4xlarge': AwsInstance(16, 30, 2, 160, True),
    'c3.8xlarge': AwsInstance(32, 60, 2, 320, True),
    'r3.large': AwsInstance(2, 15, 1, 32, False),
    'r3.xlarge': AwsInstance(4, 30.5, 1, 80, True),
    'r3.2xlarge': AwsInstance(8, 61, 1, 160, True),
    'r3.4xlarge': AwsInstance(16, 122, 1, 320, True),
    'r3.8xlarge': AwsInstance(32, 244, 2, 320, True),
}

config_file_name = dirname(__file__) + '/chiles.settings'
if exists(config_file_name):
    config = ConfigObj(config_file_name)

    # Get the AWS details
    AWS_AMI_ID = config['ami_id']
    AWS_KEY_NAME = config['key_name']
    AWS_SECURITY_GROUPS = config['security_groups']
    AWS_REGION = config['region']
    AWS_SUBNETS = config['subnets']

    BASH_SCRIPT_CVEL = config['bash_script_cvel']
    BASH_SCRIPT_CLEAN = config['bash_script_clean']
    BASH_SCRIPT_CLEAN_02 = config['bash_script_clean_02']
    BASH_SCRIPT_CLEAN_ALL = config['bash_script_clean_all']
    BASH_SCRIPT_MAKECUBE = config['bash_script_makecube']

    OBS_IDS = config['obs_ids']
