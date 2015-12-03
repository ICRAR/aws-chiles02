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
The helper for starting EC2 Instances
"""
from os.path import join, expanduser, exists
import time
import datetime

import boto
from boto.ec2.blockdevicemapping import BlockDeviceType
from boto.exception import EC2ResponseError
from boto.ec2 import blockdevicemapping

from common import make_safe_filename, LOGGER
from settings_file import AWS_SUBNETS, AWS_KEY_NAME, AWS_SECURITY_GROUPS, AWS_REGION


class CancelledException(Exception):
    """
    The request has been cancelled
    """
    pass


class EC2Helper:
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None):
        """
        Get an EC2 connection
        """
        if aws_access_key_id is not None and aws_secret_access_key is not None:
            LOGGER.info("Using user provided keys")
            self.ec2_connection = boto.ec2.connect_to_region(AWS_REGION, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        elif exists(join(expanduser('~'), '.aws/credentials')):
            # This relies on a ~/.aws/credentials file holding the '<aws access key>', '<aws secret key>'
            LOGGER.info("Using ~/.aws/credentials")
            self.ec2_connection = boto.ec2.connect_to_region(AWS_REGION, profile_name='chiles')
        else:
            # This relies on a ~/.boto or /etc/boto.cfg file holding the '<aws access key>', '<aws secret key>'
            LOGGER.info("Using ~/.boto or /etc/boto.cfg")
            self.ec2_connection = boto.ec2.connect_to_region(AWS_REGION)

    @staticmethod
    def build_block_device_map(ephemeral, number_ephemeral_disks=1, ebs_size=None, iops=None, number_ebs_volumes=1):
        bdm = blockdevicemapping.BlockDeviceMapping()

        if ephemeral:
            # The ephemeral disk
            xvdb = BlockDeviceType()
            xvdb.ephemeral_name = 'ephemeral0'
            bdm['/dev/xvdb'] = xvdb

            if number_ephemeral_disks == 2:
                xvdc = BlockDeviceType()
                xvdc.ephemeral_name = 'ephemeral1'
                bdm['/dev/xvdc'] = xvdc

        if ebs_size:
            for disks in range(0, number_ebs_volumes):
                xvd_n = blockdevicemapping.EBSBlockDeviceType(delete_on_termination=True)
                xvd_n.size = int(ebs_size)  # size in Gigabytes
                if iops:
                    xvd_n.iops = 500
                    xvd_n.volume_type = 'io1'
                else:
                    xvd_n.volume_type = 'gp2'
                last_char = chr(ord('f') + disks)
                bdm['/dev/xvd' + last_char] = xvd_n

        return bdm

    def run_instance(self, ami_id, user_data, instance_type, volume_id, created_by, name, zone, ephemeral=False):
        """
        Run up an instance
        """
        bdm = self.build_block_device_map(ephemeral)

        LOGGER.info('Running instance: ami: {0}'.format(ami_id))
        reservations = self.ec2_connection.run_instances(ami_id,
                                                         instance_type=instance_type,
                                                         instance_initiated_shutdown_behavior='terminate',
                                                         subnet_id=AWS_SUBNETS[zone],
                                                         key_name=AWS_KEY_NAME,
                                                         security_group_ids=AWS_SECURITY_GROUPS,
                                                         user_data=user_data,
                                                         block_device_map=bdm)
        instance = reservations.instances[0]
        time.sleep(5)

        while not instance.update() == 'running':
            LOGGER.info('Not running yet')
            time.sleep(5)

        if volume_id:
            # Now we have an instance id we can attach the disk
            self.ec2_connection.attach_volume(volume_id, instance.id, '/dev/xvdf')

        LOGGER.info('Assigning the tags')
        self.ec2_connection.create_tags([instance.id],
                                        {'AMI': '{0}'.format(ami_id),
                                         'Name': '{0}'.format(name),
                                         'Volume_id': '{0}'.format(volume_id),
                                         'Created By': '{0}'.format(created_by)})

        return instance

    def run_spot_instance(
            self,
            ami_id,
            spot_price,
            user_data,
            instance_type,
            volume_id,
            created_by,
            name,
            instance_details,
            zone,
            ephemeral=False,
            ebs_size=None,
            number_ebs_volumes=None,
            bdm=None):
        """
        Run the ami as a spot instance
        """
        subnet_id = AWS_SUBNETS[zone]
        now_plus = datetime.datetime.utcnow() + datetime.timedelta(minutes=5)
        if bdm is None:
            bdm = self.build_block_device_map(
                ephemeral,
                instance_details.number_disks,
                ebs_size=ebs_size,
                iops=instance_details.iops_support,
                number_ebs_volumes=number_ebs_volumes)
        spot_request = self.ec2_connection.request_spot_instances(
            spot_price,
            image_id=ami_id,
            count=1,
            valid_until=now_plus.isoformat(),
            instance_type=instance_type,
            subnet_id=subnet_id,
            key_name=AWS_KEY_NAME,
            ebs_optimized=True if instance_details.iops_support else False,
            security_group_ids=AWS_SECURITY_GROUPS,
            user_data=user_data,
            block_device_map=bdm)

        # Wait for EC2 to provision the instance
        time.sleep(10)
        instance_id = None
        error_count = 0

        # Has it been provisioned yet - we allow 3 errors before aborting
        while instance_id is None and error_count < 3:
            spot_request_id = spot_request[0].id
            requests = None
            try:
                requests = self.ec2_connection.get_all_spot_instance_requests(request_ids=[spot_request_id])
            except EC2ResponseError:
                LOGGER.exception('Error count = {0}'.format(error_count))
                error_count += 1

            if requests is None:
                # Wait for AWS to catch up
                time.sleep(10)
            else:
                LOGGER.info('{0}, state: {1}, status:{2}'.format(spot_request_id, requests[0].state, requests[0].status))
                if requests[0].state == 'active' and requests[0].status.code == 'fulfilled':
                    instance_id = requests[0].instance_id
                elif requests[0].state == 'cancelled':
                    raise CancelledException('Request {0} cancelled. Status: {1}'.format(spot_request_id, requests[0].status))
                elif requests[0].state == 'failed':
                    raise CancelledException('Request {0} failed. Status: {1}. Fault: {2}'.format(spot_request_id, requests[0].status, requests[0].fault))
                else:
                    time.sleep(10)

        reservations = self.ec2_connection.get_all_instances(instance_ids=[instance_id])
        instance = reservations[0].instances[0]

        LOGGER.info('Waiting to start up')
        while not instance.update() == 'running':
            LOGGER.info('Not running yet')
            time.sleep(5)

        if volume_id:
            LOGGER.info('Attaching {0}'.format(volume_id))
            # When we have an instance id we can attach the volume
            self.ec2_connection.attach_volume(volume_id, instance_id, '/dev/xvdf')

        # Give it time to settle down
        LOGGER.info('Assigning the tags')
        self.ec2_connection.create_tags(
            [instance_id],
            {
                'AMI': '{0}'.format(ami_id),
                'Name': '{0}'.format(name),
                'Volume_id': '{0}'.format(volume_id),
                'Created By': '{0}'.format(created_by)
            })

        return instance

    def get_volume_name(self, volume_id):
        """
        Get the name of volume (if it has one)
        """
        volume_details = self.ec2_connection.get_all_volumes(volume_id)
        if volume_details and len(volume_details) == 1:
            volume = volume_details[0]
            name = volume.tags['Name']
            if name:
                return make_safe_filename(name)

        return volume_id

    def create_volume(self, snapshot_id, zone, iops=None):
        snapshot = self.ec2_connection.get_all_snapshots([snapshot_id])
        if iops is None:
            volume_type = 'gp2'
        else:
            volume_type = 'io1'
        volume = self.ec2_connection.create_volume(None, zone, snapshot=snapshot_id, volume_type=volume_type, iops=iops)
        snapshot_name = snapshot[0].tags['Name']

        self.ec2_connection.create_tags(volume.id, {'Name': 'CAN BE DELETED: ' + snapshot_name})

        return volume, snapshot_name

    def delete_volume(self, volume_id):
        volume = self.ec2_connection.get_all_volumes([volume_id])[0]
        LOGGER.info('status = {0}'.format(volume.status))
        if volume.status == 'in-use':
            # Unattach
            volume.detach()

            for i in range(0, 10):
                time.sleep(5)

                volume = self.ec2_connection.get_all_volumes([volume_id])[0]
                LOGGER.info('status = {0}'.format(volume.status))
                if volume.status == 'available':
                    break

        volume = self.ec2_connection.get_all_volumes([volume_id])[0]
        LOGGER.info('status = {0}'.format(volume.status))
        if volume.status == 'available':
            self.ec2_connection.delete_volume(volume_id)

    def get_cheapest_spot_price(self, instance_type, max_price):
        """
        Find the cheapest spot price in a zone we use

        :param instance_type:
        :return:
        """
        LOGGER.info('instance_type: {0}'.format(instance_type))
        # Get the zones we have subnets in
        availability_zones = []
        for key, value in AWS_SUBNETS.iteritems():
            availability_zones.append(key)

        prices = self.ec2_connection.get_spot_price_history(
            start_time=datetime.datetime.now().isoformat(),
            instance_type=instance_type,
            product_description='Linux/UNIX (Amazon VPC)')

        best_price = None
        for spot_price in prices:
            LOGGER.info('Spot Price {0} - {1}'.format(spot_price.price, spot_price.availability_zone))
            if spot_price.availability_zone not in availability_zones:
                # Ignore this one
                LOGGER.info('Ignoring spot price {0} - {1}'.format(spot_price.price, spot_price.availability_zone))
            elif spot_price.price != 0.0 and best_price is None:
                best_price = spot_price
            elif spot_price.price != 0.0 and spot_price.price < best_price.price:
                best_price = spot_price
        if best_price is None:
            LOGGER.info('No Spot Price')
            return None
        elif best_price.price > max_price:
            LOGGER.info('Spot Price too high')
            return None

        LOGGER.info('bid_price: {0}, spot_price: {2}, zone: {1}'.format(max_price, best_price.availability_zone, best_price.price))
        return best_price.availability_zone
