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
Generate the ingest pipe line
"""
import argparse
import logging
import os
import sys
from time import sleep

import boto3
from configobj import ConfigObj

from aws_chiles02.common import get_argument, get_aws_credentials, get_session_id
from aws_chiles02.settings_file import AWS_AMI_ID, AWS_REGION

LOG = logging.getLogger(__name__)


def create_and_generate(**kw_args):
    # Do we have boto data
    boto_data = get_aws_credentials('aws-chiles02')
    if boto_data is not None:
        session_id = get_session_id()
        # Start by creating the EFS
        client = boto3.client('efs', region_name=AWS_REGION)

        create_file_system = client.create_file_system(CreationToken=session_id)

        life_cycle_state = create_file_system['LifeCycleState']
        while life_cycle_state == 'creating':
            # Wait for AWS to provision the EFS
            sleep(10)

            describe_file_systems = client.describe_file_systems(
                CreationToken=create_file_system['CreationToken']
            )
            file_systems = describe_file_systems['FileSystems']
            life_cycle_state = file_systems[0]['LifeCycleState']

        if life_cycle_state != 'available':
            LOG.error('Could not create the EFS')


def command_interactive(args):
    LOG.info(args)
    sleep(0.5)  # Allow the logging time to print

    path_dirname, filename = os.path.split(__file__)
    config_file_name = '{0}/aws-chiles02.settings'.format(path_dirname)
    if os.path.exists(config_file_name):
        config = ConfigObj(config_file_name)
    else:
        config = ConfigObj()
        config.filename = config_file_name

    get_argument(config, 'ami', 'AMI Id', help_text='the AMI to use', default=AWS_AMI_ID)
    get_argument(config, 'bucket_name', 'Bucket name', help_text='the bucket to access', default='13b-266')
    get_argument(config, 'volume', 'Volume', help_text='the directory on the host to bind to the Docker Apps', default='/mnt/dfms/dfms_root')
    get_argument(config, 'width', 'Frequency width', data_type=int, help_text='the frequency width', default=4)
    get_argument(config, 'nodes_ingest', 'Number of ingest nodes', data_type=int, help_text='the number of ingest nodes', default=4)

    # Write the arguments
    config.write()

    create_and_generate(
        **config
    )


def command_create(args):
    create_and_generate(
        **vars(args)
    )


def parser_arguments(command_line=sys.argv[1:]):
    parser = argparse.ArgumentParser('Build and deploy the ingest, which will trigger of Daliuge nodes')

    subparsers = parser.add_subparsers()

    parser_create = subparsers.add_parser('create', help='run and deploy')
    parser_create.add_argument('ami', help='the ami to use')
    parser_create.add_argument('spot_price1', type=float, help='the spot price for the i2.2xlarge instances')
    parser_create.add_argument('spot_price2', type=float, help='the spot price for the i2.4xlarge instances')
    parser_create.add_argument('bucket', help='the bucket to access')
    parser_create.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    parser_create.add_argument('width', type=int, help='the frequency width', default=4)
    parser_create.add_argument('-v', '--verbosity', action='count', default=0, help='increase output verbosity')
    parser_create.add_argument('-d', '--days_per_node', type=int, help='the number of days per node', default=1)
    parser_create.set_defaults(func=command_create)

    parser_interactive = subparsers.add_parser('interactive', help='prompt the user for parameters and then run')
    parser_interactive.set_defaults(func=command_interactive)

    args = parser.parse_args(command_line)
    return args


if __name__ == '__main__':
    # interactive
    arguments = parser_arguments()
    arguments.func(arguments)
