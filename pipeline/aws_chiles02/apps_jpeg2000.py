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
My Docker Apps
"""
import logging
import os

import boto3
from boto3.s3.transfer import S3Transfer

from aws_chiles02.apps_general import ErrorHandling
from aws_chiles02.common import ProgressPercentage
from dfms.drop import BarrierAppDROP

LOG = logging.getLogger(__name__)
logging.getLogger('boto3').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('nose').setLevel(logging.INFO)


class CopyFitsFromS3(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        super(CopyFitsFromS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyFitsFromS3, self).initialize(**kwargs)

    def dataURL(self):
        return 'CopyFitsFromS3'

    def run(self):
        s3_input = self.inputs[0]
        bucket_name = s3_input.bucket
        key = s3_input.key

        fits_file_name = self.outputs[0].path

        LOG.info('bucket: {0}, key: {1}, file: {2}'.format(bucket_name, key, fits_file_name))

        # Does the directory exist
        if os.path.exists(fits_file_name):
            LOG.info('fits_file: {0} exists'.format(fits_file_name))
            return 0

        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)
        s3_object = s3.Object(bucket_name, key)
        s3_size = s3_object.content_length
        s3_client = s3.meta.client
        transfer = S3Transfer(s3_client)
        transfer.download_file(
                bucket_name,
                key,
                fits_file_name,
                callback=ProgressPercentage(
                    key,
                    s3_size
                )
        )
        if not os.path.exists(fits_file_name):
            self.send_error_message(
                'The fits file {0} does not exist'.format(fits_file_name),
                LOG
            )
            return 1

        return 0


class CopyJpeg2000ToS3(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._width = None
        self._iterations = None
        super(CopyJpeg2000ToS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyJpeg2000ToS3, self).initialize(**kwargs)

    def dataURL(self):
        return 'CopyJpeg2000ToS3'

    def run(self):
        s3_output = self.outputs[0]
        bucket_name = s3_output.bucket
        key = s3_output.key

        jpeg_file_name = self.inputs[0].path

        LOG.info('file: {2}, bucket: {0}, key: {1}'.format(bucket_name, key, jpeg_file_name))

        # Does the file exists
        LOG.info('checking {0} exists'.format(jpeg_file_name))
        if not os.path.exists(jpeg_file_name):
            self.send_error_message(
                'File: {0} does not exist'.format(jpeg_file_name),
                LOG
            )
            return 0

        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)

        s3_client = s3.meta.client
        transfer = S3Transfer(s3_client)
        transfer.upload_file(
            jpeg_file_name,
            bucket_name,
            key,
            callback=ProgressPercentage(
                    key,
                    float(os.path.getsize(jpeg_file_name))
            ),
            extra_args={
                'StorageClass': 'REDUCED_REDUNDANCY',
            }
        )

        return 0
