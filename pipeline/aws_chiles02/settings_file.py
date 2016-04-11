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
Settings file
"""
from os.path import expanduser, dirname, exists

from configobj import ConfigObj

INPUT_MS_SUFFIX = '_calibrated_deepfield.ms'
INPUT_MS_SUFFIX_TAR = '_calibrated_deepfield.ms.tar'
CONTAINER_CHILES02 = 'kevinvinsen/chiles02:latest'
CONTAINER_SV = 'slavakitaeff/sv:1.4'
SIZE_1GB = 1073741824
QUEUE = 'startup_complete'
DIM_PORT = 8001

AWS_KEY = expanduser('~/.ssh/aws-chiles02-oregon.pem')
USERNAME = 'ec2-user'

AWS_AMI_ID = None
AWS_KEY_NAME = None
AWS_SECURITY_GROUPS = None
AWS_REGION = None
AWS_SUBNETS = None


config_file_name = dirname(__file__) + '/chiles02.settings'
if exists(config_file_name):
    config = ConfigObj(config_file_name)

    # Get the AWS details
    AWS_AMI_ID = config['ami_id']
    AWS_KEY_NAME = config['key_name']
    AWS_SECURITY_GROUPS = config['security_groups']
    AWS_REGION = config['region']
    AWS_SUBNETS = config['subnets']
