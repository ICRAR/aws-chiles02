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
import shutil

import boto3
from boto3.s3.transfer import S3Transfer

from aws_chiles02.common import run_command, ProgressPercentage
from dfms.apps.dockerapp import DockerApp
from dfms.drop import BarrierAppDROP

LOG = logging.getLogger(__name__)
logging.getLogger('boto3').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('nose').setLevel(logging.INFO)


class CopyFitsFromS3(BarrierAppDROP):
    def __init__(self, oid, uid, **kwargs):
        """
        initial the class, make sure super is called after the event as it calls initialize
        :param oid:
        :param uid:
        :param kwargs:
        :return:
        """
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
            LOG.error('The fits file {0} does not exist'.format(fits_file_name))
            return 1

        return 0


class CopyJpeg2000ToS3(BarrierAppDROP):
    def __init__(self, oid, uid, **kwargs):
        self._width = None
        self._iterations = None
        super(CopyJpeg2000ToS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyJpeg2000ToS3, self).initialize(**kwargs)
        self._width = self._getArg(kwargs, 'width ', None)
        self._iterations = self._getArg(kwargs, 'iterations', None)

    def dataURL(self):
        return 'CopyJpeg2000ToS3'

    def run(self):
        measurement_set_output = self.inputs[0]
        measurement_set_dir = measurement_set_output.path

        s3_output = self.outputs[0]
        bucket_name = s3_output.bucket
        key = s3_output.key
        LOG.info('dir: {2}, bucket: {0}, key: {1}'.format(bucket_name, key, measurement_set_dir))
        # Does the file exists
        stem_name = 'image_{0}_{1}'.format(self._width, self._iterations)
        measurement_set = os.path.join(measurement_set_dir, stem_name)
        LOG.info('checking {0}.cube exists'.format(measurement_set))
        if not os.path.exists(measurement_set + '.cube') or not os.path.isdir(measurement_set + '.cube'):
            LOG.info('Measurement_set: {0}.cube does not exist'.format(measurement_set))
            return 0

        # Make the tar file
        tar_filename = os.path.join(measurement_set_dir, 'image_{0}_{1}.cube.tar'.format(self._width, self._iterations))
        os.chdir(measurement_set_dir)
        bash = 'tar -cvf {0} {1}.cube'.format(tar_filename, stem_name)
        return_code = run_command(bash)
        path_exists = os.path.exists(tar_filename)
        if return_code != 0 or not path_exists:
            LOG.error('tar return_code: {0}, exists: {1}'.format(return_code, path_exists))

        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)

        s3_client = s3.meta.client
        transfer = S3Transfer(s3_client)
        transfer.upload_file(
            tar_filename,
            bucket_name,
            key,
            callback=ProgressPercentage(
                    key,
                    float(os.path.getsize(tar_filename))
            ),
            extra_args={
                'StorageClass': 'REDUCED_REDUNDANCY',
            }
        )

        # Clean up
        shutil.rmtree(measurement_set_dir, ignore_errors=True)

        return return_code
