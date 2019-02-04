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
Collect EC2 metric data
"""
import csv
import logging
import os
from datetime import datetime, timedelta

import boto3
import requests
from boto3.s3.transfer import S3Transfer

from aws_chiles02.apps_general import ErrorHandling
from aws_chiles02.common import ProgressPercentage
from dlg.drop import BarrierAppDROP

LOG = logging.getLogger(__name__)
logging.getLogger('boto3').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('nose').setLevel(logging.INFO)
logging.getLogger('s3transfer').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

METRICS = [
    'CPUUtilization',
    'NetworkIn',
    'NetworkOut',
    'NetworkPacketsIn',
    'NetworkPacketsOut',
    'DiskWriteBytes',
    'DiskReadBytes',
    'DiskWriteOps',
    'DiskReadOps',
    'StatusCheckFailed',
    'StatusCheckFailed_Instance',
    'StatusCheckFailed_System']


class EC2Metrics(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        super(EC2Metrics, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(EC2Metrics, self).initialize(**kwargs)
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def dataURL(self):
        return 'EC2Metrics'

    def run(self):
        output_file = self.outputs[0]

        session = boto3.Session(profile_name='aws-chiles02')

        instance_id = requests.get('http://169.254.169.254/latest/meta-data/instance-id').content
        ec2 = boto3.resource('ec2')
        instance = ec2.Instance(instance_id)

        cloud_watch = session.client('cloudwatch')
        now = datetime.utcnow()
        start_time = instance.launch_time
        now_plus_10 = now + timedelta(minutes=10)

        with open(output_file, 'wb') as csv_file:
            # Write the header
            csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow([
                'metric',
                'timestamp',
                'average',
                'max',
                'min',
            ])

            # Get the data
            for metric in METRICS:
                results = cloud_watch.get_metric_statistics(
                    Namespace='AWS/EC2',
                    MetricName=metric,
                    Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                    StartTime=start_time,
                    EndTime=now_plus_10,
                    Period=300,
                    Statistics=['Average', 'Max', 'Min'])

                for data_point in results['Datapoints']:
                    csv_writer.writerow([
                        metric,
                        data_point['Timestamp'],
                        data_point['Average'], data_point['Max'], data_point['Min']
                    ])


class CopyMetricsToS3(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._command = None
        super(CopyMetricsToS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyMetricsToS3, self).initialize(**kwargs)
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def dataURL(self):
        return 'CopyMetricsToS3'

    def run(self):
        input_file = self.inputs[0]
        s3_output = self.outputs[0]
        bucket_name = s3_output.bucket
        key = s3_output.key
        LOG.info('file: {2}, bucket: {0}, key: {1}'.format(bucket_name, key, input_file))

        # Does the file exists
        LOG.debug('checking {0} exists'.format(input_file))
        if not os.path.exists(input_file) or not os.path.isfile(input_file):
            LOG.warn('Metrics file: {0} does not exist'.format(input_file))
            return 0

        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)

        s3_client = s3.meta.client
        transfer = S3Transfer(s3_client)
        transfer.upload_file(
            input_file,
            bucket_name,
            key,
            callback=ProgressPercentage(
                key,
                float(os.path.getsize(input_file))
            ),
            extra_args={
                'StorageClass': 'REDUCED_REDUNDANCY',
            }
        )

        return 0
