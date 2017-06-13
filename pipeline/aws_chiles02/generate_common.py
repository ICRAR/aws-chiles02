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
The common generate code
"""
import json
import logging
import time

import boto3

from aws_chiles02.settings_file import AWS_REGION, QUEUE

LOG = logging.getLogger(__name__)


def get_reported_running(uuid, count, wait=600):
    session = boto3.Session(profile_name='aws-chiles02')
    sqs = session.resource('sqs', region_name=AWS_REGION)
    queue = sqs.get_queue_by_name(QueueName=QUEUE)
    nodes_running = {}
    stop_time = time.time() + wait
    messages_received = 0
    while time.time() <= stop_time and messages_received < count:
        for message in queue.receive_messages(MaxNumberOfMessages=10, VisibilityTimeout=100, WaitTimeSeconds=10):
            json_message = message.body
            message_details = json.loads(json_message)
            if message_details['uuid'] == uuid:
                ip_addresses = nodes_running.get(message_details['instance_type'])
                if ip_addresses is None:
                    ip_addresses = []
                    nodes_running[message_details['instance_type']] = ip_addresses
                ip_addresses.append(message_details)
                LOG.info('{0} - {1} has started successfully'.format(message_details['ip_address'], message_details['instance_type']))
                messages_received += 1
                message.delete()
                LOG.info('{0} of {1} started'.format(messages_received, count))

    LOG.info('The running nodes: {0}'.format(nodes_running))
    return nodes_running


def get_nodes_running(host_list):
    session = boto3.Session(profile_name='aws-chiles02')
    ec2 = session.resource('ec2', region_name=AWS_REGION)
    nodes_running = {}
    for instance in ec2.instances.filter():
        if instance.public_ip_address in host_list and instance.state['Name'] == 'running':
            message_details = {
                'ip_address': instance.public_ip_address,
                'instance_type': instance.instance_type
            }
            ip_addresses = nodes_running.get(instance.instance_type)
            if ip_addresses is None:
                ip_addresses = []
                nodes_running[instance.instance_type] = ip_addresses
            ip_addresses.append(message_details)

    LOG.info('The running nodes: {0}'.format(nodes_running))
    return nodes_running


def build_hosts(reported_running):
    hosts = []
    for values in reported_running.values():
        for value in values:
            hosts.append(value['ip_address'])

    return ','.join(hosts)
