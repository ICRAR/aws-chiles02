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


class CopyConcatenateFromS3(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        super(CopyConcatenateFromS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyConcatenateFromS3, self).initialize(**kwargs)
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def dataURL(self):
        return 'CopyConcatenateFromS3'

    def run(self):
        s3_input = self.inputs[0]
        bucket_name = s3_input.bucket
        key = s3_input.key

        measurement_set_output = self.outputs[0]
        measurement_set_dir = measurement_set_output.path

        LOG.info('bucket: {0}, key: {1}, dir: {2}'.format(bucket_name, key, measurement_set_dir))

        # Does the directory exist
        if os.path.exists(measurement_set_dir):
            for filename in os.listdir(measurement_set_dir):
                LOG.debug('filename: {0}'.format(filename))
                if filename.endswith('.image'):
                    LOG.warn('Measurement Set: {0} exists'.format(filename))
                    return 0

        else:
            # Make the directory
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

        if return_code != 0:
            message = 'tar return_code: {0}'.format(return_code)
            LOG.error(message)
            self.send_error_message(
                message,
                self.oid,
                self.uid
            )
            return 1

        os.remove(full_path_tar_file)

        # Remove the stuff we don't need
        LOG.info('measurement_set_dir: {0}'.format(measurement_set_dir))
        for filename in os.listdir(measurement_set_dir):
            LOG.debug('filename: {0}'.format(filename))
            if filename.endswith(tuple(['.flux', '.model', '.residual', '.psf'])):
                full_name = os.path.join(measurement_set_dir, filename)
                LOG.info('full_name: {0}'.format(full_name))
                shutil.rmtree(full_name, ignore_errors=True)

        return 0


class CopyConcatenateToS3(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._width = None
        self._iterations = None
        super(CopyConcatenateToS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyConcatenateToS3, self).initialize(**kwargs)
        self._width = self._getArg(kwargs, 'width ', None)
        self._iterations = self._getArg(kwargs, 'iterations', None)
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def dataURL(self):
        return 'CopyConcatenateToS3'

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
        LOG.debug('checking {0}.cube exists'.format(measurement_set))
        if not os.path.exists(measurement_set + '.cube') or not os.path.isdir(measurement_set + '.cube'):
            message = 'Measurement_set: {0}.cube does not exist'.format(measurement_set)
            LOG.error(message)
            self.send_error_message(
                message,
                self.oid,
                self.uid
            )
            return 0

        # Make the tar file
        tar_filename = os.path.join(measurement_set_dir, 'image_{0}_{1}.cube.tar'.format(self._width, self._iterations))
        os.chdir(measurement_set_dir)
        bash = 'tar -cvf {0} {1}.cube'.format(tar_filename, stem_name)
        return_code = run_command(bash)
        path_exists = os.path.exists(tar_filename)
        if return_code != 0 or not path_exists:
            message = 'tar return_code: {0}, exists: {1}'.format(return_code, path_exists)
            LOG.error(message)
            self.send_error_message(
                message,
                self.oid,
                self.uid
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

        # Clean up
        shutil.rmtree(measurement_set_dir, ignore_errors=True)

        return return_code


class CasaConcatenate(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._measurement_sets = None
        self._width = None
        self._iterations = None
        super(CasaConcatenate, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CasaConcatenate, self).initialize(**kwargs)
        self._measurement_sets = self._getArg(kwargs, 'measurement_sets', None)
        self._width = self._getArg(kwargs, 'width', None)
        self._iterations = self._getArg(kwargs, 'iterations', None)
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def run(self):
        # Because of the lifecycle the drop isn't attached when the command is
        # created so we have to do it later
        measurement_sets = []
        for measurement_set in self._measurement_sets:
            LOG.debug('measurement_set: {0}'.format(measurement_set))
            for file_name in os.listdir(measurement_set):
                if file_name.endswith(".image"):
                    dfms_name = '{0}/{1}'.format(measurement_set, file_name)
                    LOG.info('dfms_name: {0}'.format(dfms_name))
                    measurement_sets.append(dfms_name)
                    break

        measurement_set_output = self.outputs[0]
        measurement_set_dir = measurement_set_output.path

        if os.path.exists(measurement_set_dir):
            LOG.info('Directory {0} exists'.format(measurement_set_dir))
        else:
            # Make the directory
            os.makedirs(measurement_set_dir)

        command = 'cd {0} && casa --nologger --log2term -c /home/ec2-user/aws-chiles02/pipeline/casa_code/concatenate.py /tmp image_{1}_{2}.cube {3}'.format(
            measurement_set_dir,
            self._width,
            self._iterations,
            ' '.join(measurement_sets),
        )
        return_code = run_command(command)
        return return_code

    def dataURL(self):
        return 'CasaConcatenate'


class DockerImageconcat(DockerApp, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._measurement_sets = None
        self._command = None
        self._width = None
        self._iterations = None
        super(DockerImageconcat, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerImageconcat, self).initialize(**kwargs)
        self._measurement_sets = self._getArg(kwargs, 'measurement_sets', None)
        self._width = self._getArg(kwargs, 'width', None)
        self._iterations = self._getArg(kwargs, 'iterations', None)
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
                    LOG.info('dfms_name: {0}'.format(dfms_name))
                    measurement_sets.append(dfms_name)
                    break

        self._command = 'imageconcat.sh %o0 image_{1}_{2}.cube {0}'.format(
            ' '.join(measurement_sets),
            self._width,
            self._iterations,
        )
        super(DockerImageconcat, self).run()

    def dataURL(self):
        return 'docker container chiles02:latest'
