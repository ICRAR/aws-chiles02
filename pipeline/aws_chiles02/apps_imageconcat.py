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
from aws_chiles02.common import run_command, ProgressPercentage
from dfms.apps.dockerapp import DockerApp
from dfms.drop import BarrierAppDROP

LOG = logging.getLogger(__name__)
TAR_FILE = 'ms.tar'
logging.getLogger('boto3').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('nose').setLevel(logging.INFO)
logging.getLogger('s3transfer').setLevel(logging.INFO)


class CopyImageconcatFromS3(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._max_frequency = None
        self._min_frequency = None
        super(CopyImageconcatFromS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyImageconcatFromS3, self).initialize(**kwargs)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def run(self):
        s3_input = self.inputs[0]
        bucket_name = s3_input.bucket
        key = s3_input.key

        measurement_set_output = self.outputs[0]
        measurement_set_dir = measurement_set_output.path

        LOG.info('bucket: {0}, key: {1}, dir: {2}'.format(bucket_name, key, measurement_set_dir))

        measurement_set = os.path.join(measurement_set_dir, 'cleaned_{0}~{1}'.format(self._min_frequency, self._max_frequency))
        LOG.debug('Checking {0} exists'.format(measurement_set))
        if os.path.exists(measurement_set) and os.path.isdir(measurement_set):
            LOG.warn('Measurement Set: {0} exists'.format(measurement_set))
            return 0

        # Make the directory
        if not os.path.exists(measurement_set_dir):
            os.makedirs(measurement_set_dir)

        full_path_tar_file = os.path.join(measurement_set_dir, TAR_FILE)
        LOG.info('Tar: {0}'.format(full_path_tar_file))

        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)
        s3_object = s3.Object(bucket_name, key)
        s3_size = s3_object.content_length
        s3_client = s3.meta.client
        transfer = S3Transfer(s3_client)
        transfer.download_file(
            bucket_name,
            key,
            full_path_tar_file,
            callback=ProgressPercentage(
                key,
                s3_size
            )
        )
        if not os.path.exists(full_path_tar_file):
            message = 'The tar file {0} does not exist'.format(full_path_tar_file)
            LOG.error(message)
            self.send_error_message(
                message,
                self.oid,
                self.uid
            )
            return 1

        # Check the sizes match
        tar_size = os.path.getsize(full_path_tar_file)
        if s3_size != tar_size:
            message = 'The sizes for {0} differ S3: {1}, local FS: {2}'.format(full_path_tar_file, s3_size, tar_size)
            LOG.error(message)
            self.send_error_message(
                message,
                self.oid,
                self.uid
            )
            return 1

        # The tar file exists and is the same size
        bash = 'tar -xvf {0} -C {1}'.format(full_path_tar_file, measurement_set_dir)
        return_code = run_command(bash)

        path_exists = os.path.exists(measurement_set)
        if return_code != 0 or not path_exists:
            message = 'tar return_code: {0}, exists: {1}-{2}'.format(return_code, measurement_set, path_exists)
            LOG.error(message)
            self.send_error_message(
                message,
                self.oid,
                self.uid
            )
            return 1

        os.remove(full_path_tar_file)

        return 0

    def dataURL(self):
        return 'CopyImageconcatFromS3'


class CopyImageconcatToS3(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        self._only_image = None
        super(CopyImageconcatToS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyImageconcatToS3, self).initialize(**kwargs)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def dataURL(self):
        return 'CopyImageconcatToS3'

    def run(self):
        measurement_set_output = self.inputs[0]
        measurement_set_dir = measurement_set_output.path

        s3_output = self.outputs[0]
        bucket_name = s3_output.bucket
        key = s3_output.key
        LOG.info('dir: {2}, bucket: {0}, key: {1}'.format(bucket_name, key, measurement_set_dir))

        # Does the file exists
        stem_name = 'imageconcat_{0}~{1}'.format(self._min_frequency, self._max_frequency)
        measurement_set = os.path.join(measurement_set_dir, stem_name)
        LOG.debug('checking {0}.image exists'.format(measurement_set))
        if not os.path.exists(measurement_set + '.image') or not os.path.isdir(measurement_set + '.image'):
            message = 'Measurement_set: {0}.image does not exist'.format(measurement_set)
            LOG.error(message)
            self.send_error_message(
                message,
                self.oid,
                self.uid
            )
            return 0

        # Make the tar file
        tar_filename = os.path.join(measurement_set_dir, 'clean_{0}~{1}.tar'.format(self._min_frequency, self._max_frequency))
        os.chdir(measurement_set_dir)
        bash = 'tar -cvf {0} {1}.image'.format(
            tar_filename,
            stem_name,
        )
        return_code = run_command(bash)
        path_exists = os.path.exists(tar_filename)
        if return_code != 0 or not path_exists:
            message = 'tar return_code: {0}, exists: {1}'.format(return_code, path_exists)
            LOG.error(message)
            self.send_error_message(
                message,
                self.oid,
                self.uid,
            )

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

        return return_code


class CopyFitsToS3(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        super(CopyFitsToS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyFitsToS3, self).initialize(**kwargs)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def dataURL(self):
        return 'CopyFitsToS3'

    def run(self):
        measurement_set_output = self.inputs[0]
        measurement_set_dir = measurement_set_output.path

        s3_output = self.outputs[0]
        bucket_name = s3_output.bucket
        key = s3_output.key
        LOG.info('dir: {2}, bucket: {0}, key: {1}'.format(bucket_name, key, measurement_set_dir))
        # Does the file exists
        stem_name = 'clean_{0}~{1}'.format(self._min_frequency, self._max_frequency)
        measurement_set = os.path.join(measurement_set_dir, stem_name)
        LOG.debug('checking {0}.fits exists'.format(measurement_set))
        fits_file = measurement_set + '.fits'
        if not os.path.exists(fits_file) or not os.path.isfile(fits_file):
            LOG.warn('Measurement_set: {0}.fits does not exist'.format(measurement_set))
            return 0

        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)

        s3_client = s3.meta.client
        transfer = S3Transfer(s3_client)
        transfer.upload_file(
            fits_file,
            bucket_name,
            key,
            callback=ProgressPercentage(
                key,
                float(os.path.getsize(fits_file))
            ),
            extra_args={
                'StorageClass': 'REDUCED_REDUNDANCY',
            }
        )

        return 0


class DockerImageconcat(DockerApp, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._measurement_sets = None
        self._command = None
        self._max_frequency = None
        self._min_frequency = None
        super(DockerImageconcat, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerImageconcat, self).initialize(**kwargs)
        self._measurement_sets = self._getArg(kwargs, 'measurement_sets', None)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._command = 'imageconcat.sh'
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def run(self):
        # Because of the lifecycle the drop isn't attached when the command is
        # created so we have to do it later
        measurement_sets = []
        for measurement_set in self._measurement_sets:
            LOG.debug('measurement_set: {0}'.format(measurement_set))
            for file_name in os.listdir(measurement_set):
                if file_name.endswith(".image"):
                    dfms_name = '/dfms_root{0}/{1}'.format(measurement_set, file_name)
                    measurement_sets.append(dfms_name)
                    break

        self._command = 'imageconcat.sh %o0 image_{0}_{1}.cube {2}'.format(
            self._min_frequency,
            self._max_frequency,
            ' '.join(measurement_sets),
        )
        super(DockerImageconcat, self).run()

    def dataURL(self):
        return 'docker container chiles02:latest'
