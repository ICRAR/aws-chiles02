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
Test the Boto3 EC2 controller
"""
import argparse
import base64
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from aws_chiles02.common import get_aws_credentials
from aws_chiles02.ec2_controller import EC2Controller
from aws_chiles02.settings_file import AWS_AMI_ID

LOG = logging.getLogger(__name__)


def command_test01(args):
    ec2_controller = EC2Controller(
        AWS_AMI_ID,
        [
            {
                'number_instances': 2,
                'instance_type': 'm4.large',
                'spot_price': 0.03
            }
        ],
        '',
        'us-west-2',
        tags=[
            {
                'Key': 'Owner',
                'Value': 'Kevin',
            },
            {
                'Key': 'Name',
                'Value': 'Test01',
            }
        ]
    )
    ec2_controller.start_instances()


def command_test02(args):
    yaml = get_file_contents('dfms_cloud_init.yaml')
    boto_data = get_aws_credentials('aws-chiles02')
    if boto_data is not None:
        yaml = yaml.format(
            'aws-chiles02',
            boto_data[0],
            boto_data[1],
        )
        user_data = MIMEMultipart()
        user_data.attach(MIMEText(yaml))
        user_data.attach(MIMEText('''#!/bin/bash -vx
df -h
'''))
        encode = user_data.as_string().encode("ascii")
        encoded_data = base64.b64encode(encode).decode('ascii')
        ec2_controller = EC2Controller(
            AWS_AMI_ID,
            [
                {
                    'number_instances': 1,
                    'instance_type': 'm4.large',
                    'spot_price': 0.03
                }
            ],
            encoded_data,
            'us-west-2',
            tags=[
                {
                    'Key': 'Owner',
                    'Value': 'Kevin',
                },
                {
                    'Key': 'Name',
                    'Value': 'Test02',
                }
            ]
        )
        ec2_controller.start_instances()


def command_test03(args):
    boto_data = get_aws_credentials('aws-chiles02')
    if boto_data is not None:
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
        user_script = get_file_contents('node_manager_start_up.bash')
        dynamic_script = '#!/bin/bash -vx\n' \
                         'runuser -l ec2-user -c \'cd /home/ec2-user/aws-chiles02/pipeline/aws_chiles02 && source /home/ec2-user/virtualenv/aws-chiles02/bin/activate && ' \
                         'python startup_complete.py startup_complete us-west-2 "Hello world"\''
        user_data = get_user_data([cloud_init, cloud_init_dynamic, user_script, dynamic_script])
        ec2_controller = EC2Controller(
            AWS_AMI_ID,
            [
                {
                    'number_instances': 1,
                    'instance_type': 'i2.xlarge',
                    'spot_price': 0.10
                }
            ],
            user_data,
            'us-west-2',
            tags=[
                {
                    'Key': 'Owner',
                    'Value': 'Kevin',
                },
                {
                    'Key': 'Name',
                    'Value': 'Test03',
                }
            ]
        )
        ec2_controller.start_instances()


def command_test04(args):
    ec2_controller = EC2Controller(
        AWS_AMI_ID,
        [
            {
                'number_instances': 8,
                'instance_type': 'c4.large',
                'spot_price': 0.05
            },
            {
                'number_instances': 12,
                'instance_type': 'm4.large',
                'spot_price': 0.03
            },
        ],
        '',
        'us-west-2',
        tags=[
            {
                'Key': 'Owner',
                'Value': 'Kevin',
            },
            {
                'Key': 'Name',
                'Value': 'Test04',
            }
        ]
    )
    ec2_controller.start_instances()


def parser_arguments():
    parser = argparse.ArgumentParser('Test spot instances')

    subparsers = parser.add_subparsers()

    parser_test01 = subparsers.add_parser('test01', help='Just start a spot instance')
    parser_test01.set_defaults(func=command_test01)

    parser_test02 = subparsers.add_parser('test02', help='Spot instances with YAML')
    parser_test02.set_defaults(func=command_test02)

    parser_test03 = subparsers.add_parser('test03', help='Spot instances with YAML & User data')
    parser_test03.set_defaults(func=command_test03)

    parser_test04 = subparsers.add_parser('test04', help='Test lots of Spot instances')
    parser_test04.set_defaults(func=command_test04)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    arguments = parser_arguments()
    arguments.func(arguments)
