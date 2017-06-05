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
Startup is complete so put a message on the queue
"""
import argparse
import json
import logging
import urllib2

import boto3

LOG = logging.getLogger(__name__)


def parser_arguments():
    parser = argparse.ArgumentParser('Put a message on the queue')
    parser.add_argument('queue', help='the queue')
    parser.add_argument('region', help='the region')
    parser.add_argument('uuid', help='the uuid')

    args = parser.parse_args()
    return args


def build_file(args):
    session = boto3.Session(profile_name='aws-chiles02')
    sqs = session.resource('sqs', region_name=args.region)
    queue = sqs.get_queue_by_name(QueueName=args.queue)

    # Load the public IP address
    ip_address = urllib2.urlopen('http://169.254.169.254/latest/meta-data/public-ipv4').read()
    instance_type = urllib2.urlopen('http://169.254.169.254/latest/meta-data/instance-type').read()
    message = {
        'ip_address': ip_address,
        'uuid': args.uuid,
        'instance_type': instance_type,
    }
    json_message = json.dumps(message, indent=2)
    queue.send_message(
        MessageBody=json_message,
    )

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    arguments = parser_arguments()
    build_file(arguments)
