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
The database server
"""
import logging

import boto3

LOG = logging.getLogger(__name__)


class DatabaseServer(object):
    def __init__(self, instance_id, region):
        self._instance_id = instance_id

        session = boto3.Session(profile_name='aws-chiles02')
        self._ec2 = session.resource('ec2', region_name=region)
        self._ec2_client = self._ec2.meta.client

    def is_running(self):
        instance = self._ec2.Instance(self._instance_id)
        if instance.state['Name'] == 'running':
            return True
        return False

    def is_stopped(self):
        instance = self._ec2.Instance(self._instance_id)
        if instance.state['Name'] == 'stopped':
            return True
        return False

    def start(self):
        self._ec2.Instance(self._instance_id).start()

    def stop(self):
        self._ec2.Instance(self._instance_id).stop()

    def is_terminated(self):
        instance = self._ec2.Instance(self._instance_id)
        if instance.state['Name'] == 'terminated':
            return True
        return False

    def is_shutting_down(self):
        instance = self._ec2.Instance(self._instance_id)
        if instance.state['Name'] == 'stopping' or instance.state['Name'] == 'shutting-down':
            return True
        return False

    @property
    def ip_address(self):
        instance = self._ec2.Instance(self._instance_id)
        return instance.public_ip_address
