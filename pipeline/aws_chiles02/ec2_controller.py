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
An EC2 Controller
"""
import datetime
import logging
import time

import boto3

from aws_chiles02.settings_file import AWS_KEY_NAME, AWS_SECURITY_GROUPS, AWS_SUBNETS

LOG = logging.getLogger(__name__)


class CancelledException(Exception):
    """
    The request has been cancelled
    """
    pass


class BidPriceException(Exception):
    """
    The bid price is too low
    """
    pass


class EC2Controller(object):
    def __init__(self, ami_id, instances_required, user_data, region, tags=None):
        self._ami_id = ami_id
        self._instances_required = instances_required
        self._user_data = user_data
        self._tags = tags

        session = boto3.Session(profile_name='aws-chiles02')
        self._ec2 = session.resource('ec2', region_name=region)
        self._ec2_client = self._ec2.meta.client

    def start_instances(self):
        for instance_required in self._instances_required:
            self._start_instances(
                instance_required['instance_type'],
                instance_required['number_instances'],
                instance_required['spot_price'])

    def _start_instances(self, instance_type, total_number_instances, spot_price, start_spots_step=10):
        for node_id in range(0, total_number_instances, start_spots_step):
            if node_id + start_spots_step < total_number_instances:
                number_instances = start_spots_step
            else:
                number_instances = total_number_instances - node_id

            zone = self._get_cheapest_availability_zone(instance_type, spot_price)

            if zone is None:
                raise BidPriceException('Bid price too low')

            now_plus = datetime.datetime.utcnow() + datetime.timedelta(minutes=10)
            spot_request = self._ec2_client.request_spot_instances(
                    SpotPrice=str(spot_price),
                    InstanceCount=number_instances,
                    ValidUntil=now_plus.isoformat(),
                    LaunchSpecification=self._build_launch_specification(zone, instance_type)
            )
            # Wait for EC2 to provision the instance
            time.sleep(10)
            instance_ids = [None] * number_instances
            spot_instance_request_ids = [i['SpotInstanceRequestId'] for i in spot_request['SpotInstanceRequests']]
            error_count = 0

            while len(tuple(filter(lambda x: x is None, instance_ids))) > 0 and error_count < 3:
                requests = None
                # noinspection PyBroadException
                try:
                    requests = self._ec2_client.describe_spot_instance_requests(
                            SpotInstanceRequestIds=spot_instance_request_ids
                    )
                except Exception:
                    LOG.exception('Error count = {0}'.format(error_count))
                    error_count += 1
                if requests is None:
                    # Wait for AWS to catch up
                    time.sleep(10)
                else:
                    count = 0
                    for request_status in requests['SpotInstanceRequests']:
                        if request_status['State'] == 'active' and request_status['Status']['Code'] == 'fulfilled':
                            LOG.info(
                                '{0}, state: {1}, status:{2}'.format(
                                    request_status['SpotInstanceRequestId'],
                                    request_status['State'],
                                    request_status['Status']['Code']
                                )
                            )
                            instance_ids[count] = request_status['InstanceId']
                        elif request_status['State'] == 'cancelled':
                            LOG.warning('Request {0} cancelled. Status: {1}'.format(
                                    request_status['SpotInstanceRequestId'],
                                    request_status['Status']['Code']))
                            if request_status['Status']['Code'] == 'request-canceled-and-instance-running':
                                instance_ids[count] = request_status['InstanceId']
                            else:
                                instance_ids[count] = 'cancelled'
                        elif request_status['State'] == 'failed':
                            LOG.warning('Request {0} failed. Status: {1}. Fault: {2}'.format(
                                    request_status['SpotInstanceRequestId'],
                                    request_status['Status']['Code'],
                                    request_status['Status']['Message'])
                            )
                            instance_ids[count] = 'failed'
                        elif request_status['State'] == 'open':
                            LOG.info(
                                '{0}, state: {1}, status:{2}'.format(
                                    request_status['SpotInstanceRequestId'],
                                    request_status['State'],
                                    request_status['Status']['Code']
                                )
                            )
                            if request_status['Status']['Code'] == 'capacity-oversubscribed':
                                instance_ids[count] = 'failed'
                            else:
                                instance_ids[count] = None
                        else:
                            LOG.info(
                                '{0}, state: {1}, status:{2}'.format(
                                    request_status['SpotInstanceRequestId'],
                                    request_status['State'],
                                    request_status['Status']['Code']
                                )
                            )
                            instance_ids[count] = None
                        count += 1
                    time.sleep(20)

            if self._tags is not None:
                valid_instance_ids = tuple(filter(lambda x: x != 'failed' and x != 'cancelled', instance_ids))
                if len(valid_instance_ids) > 0:
                    self._ec2_client.create_tags(
                            Resources=valid_instance_ids,
                            Tags=self._tags
                    )

    def _get_cheapest_availability_zone(self, instance_type, bid_spot_price):
        # Get the zones we have subnets in
        availability_zones = []
        for key, value in AWS_SUBNETS.items():
            availability_zones.append(key)

        prices = self._ec2_client.describe_spot_price_history(
            StartTime=datetime.datetime.now().isoformat(),
            InstanceTypes=[instance_type],
            ProductDescriptions=['Linux/UNIX (Amazon VPC)'],
        )

        best_price = None
        for spot_price in prices['SpotPriceHistory']:
            LOG.info('Spot Price {0} - {1}'.format(spot_price['SpotPrice'], spot_price['AvailabilityZone']))
            if spot_price['AvailabilityZone'] not in availability_zones:
                # Ignore this one
                LOG.info(
                    'Ignoring spot price {0} - {1}'.format(spot_price['SpotPrice'], spot_price['AvailabilityZone']))
            elif spot_price['SpotPrice'] != 0.0 and best_price is None:
                best_price = spot_price
            elif spot_price['SpotPrice'] != 0.0 and spot_price['SpotPrice'] < best_price['SpotPrice']:
                best_price = spot_price
        if best_price is None:
            LOG.info('No Spot Price')
            return None
        elif float(best_price['SpotPrice']) > bid_spot_price:
            LOG.info('Spot Price higher than bid price')
            return None

        LOG.info('bid_price: {0}, spot_price: {1}, zone: {2}'.format(
            bid_spot_price,
            best_price['SpotPrice'],
            best_price['AvailabilityZone'])
        )
        return best_price['AvailabilityZone']

    def _build_launch_specification(self, zone, instance_type):
        specification = {
            'ImageId': self._ami_id,
            'KeyName': AWS_KEY_NAME,
            'SecurityGroupIds': [AWS_SECURITY_GROUPS],
            'UserData': self._user_data,
            'InstanceType': instance_type,
            'SubnetId': AWS_SUBNETS[zone],
        }
        if instance_type == 'i2.2xlarge':
            specification['BlockDeviceMappings'] = [
                {
                    "DeviceName": "/dev/sdb",
                    "VirtualName": "ephemeral0"
                },
                {
                    "DeviceName": "/dev/sdc",
                    "VirtualName": "ephemeral1"
                },
            ]
        elif instance_type == 'i2.xlarge':
            specification['BlockDeviceMappings'] = [
                {
                    "DeviceName": "/dev/sdb",
                    "VirtualName": "ephemeral0"
                },
            ]
        elif instance_type == 'i2.4xlarge':
            specification['BlockDeviceMappings'] = [
                {
                    "DeviceName": "/dev/sdb",
                    "VirtualName": "ephemeral0"
                },
                {
                    "DeviceName": "/dev/sdc",
                    "VirtualName": "ephemeral1"
                },
                {
                    "DeviceName": "/dev/sdd",
                    "VirtualName": "ephemeral2"
                },
                {
                    "DeviceName": "/dev/sde",
                    "VirtualName": "ephemeral3"
                },
            ]
        elif instance_type == 'i2.8xlarge':
            specification['BlockDeviceMappings'] = [
                {
                    "DeviceName": "/dev/sdb",
                    "VirtualName": "ephemeral0"
                },
                {
                    "DeviceName": "/dev/sdc",
                    "VirtualName": "ephemeral1"
                },
                {
                    "DeviceName": "/dev/sdd",
                    "VirtualName": "ephemeral2"
                },
                {
                    "DeviceName": "/dev/sde",
                    "VirtualName": "ephemeral3"
                },
                {
                    "DeviceName": "/dev/sdf",
                    "VirtualName": "ephemeral4"
                },
                {
                    "DeviceName": "/dev/sdg",
                    "VirtualName": "ephemeral5"
                },
                {
                    "DeviceName": "/dev/sdh",
                    "VirtualName": "ephemeral6"
                },
                {
                    "DeviceName": "/dev/sdi",
                    "VirtualName": "ephemeral7"
                },
            ]
        return specification
