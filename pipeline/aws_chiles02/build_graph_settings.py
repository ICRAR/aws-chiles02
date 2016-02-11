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
Build the graph.settings file
"""
import logging
import argparse
import os

import boto3

LOG = logging.getLogger(__name__)


def parser_arguments():
    parser = argparse.ArgumentParser('Build the graph.settings file')
    parser.add_argument('key', help='the key on the EC2 instance we are looking for')
    parser.add_argument('value', help='the value we are looking for')
    parser.add_argument('region', help='the region to check')

    args = parser.parse_args()
    return args


def build_file(args):
    session = boto3.Session(profile_name='aws-chiles02')
    ec2 = session.resource('ec2', region_name=args.region)
    instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    node_id = 0
    list_ips = []
    with open('{0}/graph.settings'.format(os.path.dirname(__file__)), "w") as settings_file:
        for instance in instances:
            for tag in instance.tags:
                if tag['Key'] == args.key and tag['Value'] == args.value:
                    LOG.info('Found {0}, {1}, {2}'.format(instance.id, instance.instance_type, instance.public_ip_address))
                    settings_file.write('node_{0}  = "{1}"\n'.format(node_id, instance.public_ip_address))
                    node_id += 1
                    list_ips.append(instance.public_ip_address)

    LOG.info(','.join(list_ips))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    arguments = parser_arguments()
    build_file(arguments)
