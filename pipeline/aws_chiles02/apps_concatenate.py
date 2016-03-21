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
TAR_FILE = 'ms.tar'
logging.getLogger('boto3').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('nose').setLevel(logging.INFO)


class CopyConcatenateFromS3(BarrierAppDROP):
    def __init__(self, oid, uid, **kwargs):
        """
        initial the class, make sure super is called after the event as it calls initialize
        :param oid:
        :param uid:
        :param kwargs:
        :return:
        """
        super(CopyConcatenateFromS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyConcatenateFromS3, self).initialize(**kwargs)

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
                LOG.info('filename: {0}'.format(filename))
                if filename.endswith('.image'):
                    LOG.info('Measurement Set: {0} exists'.format(filename))
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
            LOG.error('The tar file {0} does not exist'.format(full_path_tar_file))
            return 1

        # Check the sizes match
        tar_size = os.path.getsize(full_path_tar_file)
        if s3_size != tar_size:
            LOG.error('The sizes for {0} differ S3: {1}, local FS: {2}'.format(full_path_tar_file, s3_size, tar_size))
            return 1

        # The tar file exists and is the same size
        bash = 'tar -xvf {0} -C {1}'.format(full_path_tar_file, measurement_set_dir)
        return_code = run_command(bash)

        if return_code != 0:
            LOG.error('tar return_code: {0}'.format(return_code))
            return 1

        os.remove(full_path_tar_file)

        # Remove the stuff we don't need
        LOG.info('measurement_set_dir: {0}'.format(measurement_set_dir))
        for filename in os.listdir(measurement_set_dir):
            LOG.info('filename: {0}'.format(filename))
            if filename.endswith(tuple(['.flux', '.model', '.residual', '.psf'])):
                full_name = os.path.join(measurement_set_dir, filename)
                LOG.info('full_name: {0}'.format(full_name))
                shutil.rmtree(full_name, ignore_errors=True)

        return 0


class CopyConcatenateToS3(BarrierAppDROP):
    def __init__(self, oid, uid, **kwargs):
        self._width = None
        self._iterations = None
        super(CopyConcatenateToS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyConcatenateToS3, self).initialize(**kwargs)
        self._width = self._getArg(kwargs, 'width ', None)
        self._iterations = self._getArg(kwargs, 'iterations', None)

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
        LOG.info('checking {0}.image exists'.format(measurement_set))
        if not os.path.exists(measurement_set + '.image') or not os.path.isdir(measurement_set + '.image'):
            LOG.info('Measurement_set: {0}.image does not exist'.format(measurement_set))
            return 0

        # Make the tar file
        tar_filename = os.path.join(measurement_set_dir, 'image_{0}_{1}.tar'.format(self._width, self._iterations))
        os.chdir(measurement_set_dir)
        bash = 'tar -cvf {0} {1}.flux {1}.image {1}.model {1}.residual {1}.psf'.format(tar_filename, stem_name)
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


class DockerConcatenate(DockerApp):
    def __init__(self, oid, uid, **kwargs):
        self._measurement_sets = None
        self._command = None
        self._width = None
        self._iterations = None
        super(DockerConcatenate, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerConcatenate, self).initialize(**kwargs)

        self._measurement_sets = self._getArg(kwargs, 'measurement_sets', None)
        self._width = self._getArg(kwargs, 'width', None)
        self._iterations = self._getArg(kwargs, 'iterations', None)
        self._command = 'concatenate.sh'

    def run(self):
        # Because of the lifecycle the drop isn't attached when the command is
        # created so we have to do it later
        measurement_sets = []
        for measurement_set in self._measurement_sets:
            LOG.info('measurement_set: {0}'.format(measurement_set))
            for file_name in os.listdir(measurement_set):
                if file_name.endswith(".image"):
                    dfms_name = '/dfms_root{0}/{1}'.format(measurement_set, file_name)
                    LOG.info('dfms_name: {0}'.format(dfms_name))
                    measurement_sets.append(dfms_name)
                    break

        self._command = 'concatenate.sh %o0/image_{1}_{2} {0}'.format(
            ' '.join(measurement_sets),
            self._width,
            self._iterations,
        )
        super(DockerConcatenate, self).run()

    def dataURL(self):
        return 'docker container chiles02:latest'
