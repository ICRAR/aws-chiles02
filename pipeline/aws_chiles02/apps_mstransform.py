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
from aws_chiles02.check_measurement_set import CheckMeasurementSet
from aws_chiles02.common import ProgressPercentage, run_command
from aws_chiles02.settings_file import CASA_COMMAND_LINE, SCRIPT_PATH
from dlg.apps.dockerapp import DockerApp
from dlg.drop import BarrierAppDROP

LOG = logging.getLogger(__name__)
TAR_FILE = 'ms.tar'
logging.getLogger('boto3').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('nose').setLevel(logging.INFO)
logging.getLogger('s3transfer').setLevel(logging.INFO)


class CopyMsTransformFromS3(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        super(CopyMsTransformFromS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyMsTransformFromS3, self).initialize(**kwargs)
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def dataURL(self):
        return 'app CopyMsTransformFromS3'

    def run(self):
        s3_input = self.inputs[0]
        bucket_name = s3_input.bucket
        key = s3_input.key

        measurement_set_output = self.outputs[0]
        measurement_set_dir = measurement_set_output.path

        LOG.info('bucket: {0}, key: {1}, dir: {2}'.format(bucket_name, key, measurement_set_dir))

        measurement_set = key[:-4]
        LOG.debug('Checking {0} exists'.format(measurement_set))
        if os.path.exists(measurement_set) and os.path.isdir(measurement_set):
            LOG.warn('Measurement Set: {0} exists'.format(measurement_set))
            return 0

        # Make the directory
        if not os.path.exists(measurement_set_dir):
            os.makedirs(measurement_set_dir)

        # The following will need (16 + 1) * 262144000 bytes of heap space, ie approximately 4.5G.
        # Note setting minimum as well as maximum heap results in OutOfMemory errors at times!
        # The -d64 is to make sure we are using a 64bit JVM.
        # When extracting to the tar we need even more
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
            message = 'tar return_code: {0}, exists: {1}, measurement_set: {2}'.format(return_code, path_exists, measurement_set)
            LOG.error(message)
            self.send_error_message(
                message,
                self.oid,
                self.uid
            )
            return 1

        os.remove(full_path_tar_file)
        return 0


class CopyMsTransformToS3(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._max_frequency = None
        self._min_frequency = None
        super(CopyMsTransformToS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyMsTransformToS3, self).initialize(**kwargs)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def dataURL(self):
        return 'app CopyMsTransformToS3'

    def run(self):
        measurement_set_output = self.inputs[0]
        measurement_set_dir = measurement_set_output.path

        s3_output = self.outputs[0]
        bucket_name = s3_output.bucket
        key = s3_output.key
        LOG.info('dir: {2}, bucket: {0}, key: {1}'.format(bucket_name, key, measurement_set_dir))

        directory_name = 'vis_{0}~{1}'.format(self._min_frequency, self._max_frequency)
        measurement_set = os.path.join(measurement_set_dir, directory_name)
        LOG.debug('check {0} exists'.format(measurement_set))
        if not os.path.exists(measurement_set) or not os.path.isdir(measurement_set):
            message = 'Measurement_set: {0} does not exist'.format(measurement_set)
            LOG.error(message)
            self.send_error_message(
                message,
                self.oid,
                self.uid
            )
            return 0

        # Make the tar file
        tar_filename = os.path.join(measurement_set_dir, 'vis.tar')
        os.chdir(measurement_set_dir)
        bash = 'tar -cvf {0} {1}'.format(tar_filename, directory_name)
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


class DockerMsTransform(DockerApp, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        super(DockerMsTransform, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerMsTransform, self).initialize(**kwargs)

        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._command = 'mstransform.sh %i0 %o0 {0} {1} {2} {3}'
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def run(self):
        # Because of the lifecycle the drop isn't attached when the command is
        # created so we have to do it later
        json_drop = self.inputs[1]
        self._command = 'mstransform.sh %i0 %o0 {0} {1} {2}'.format(
            self._min_frequency,
            self._max_frequency,
            json_drop['Bottom edge'],
        )
        super(DockerMsTransform, self).run()

        check_measurement_set = CheckMeasurementSet(
            os.path.join(
                self.outputs[0].path,
                'vis_{0}~{1}'.format(self._min_frequency, self._max_frequency)
            )
        )
        error_message = check_measurement_set.check_tables_to_24()

        if error_message is not None:
            LOG.error(error_message)
            self.send_error_message(
                error_message,
                self.oid,
                self.uid
            )
            return 1

    def dataURL(self):
        return 'docker container chiles02:latest'


class CasaMsTransform(DockerApp, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        super(CasaMsTransform, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CasaMsTransform, self).initialize(**kwargs)

        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._command = 'mstransform.sh %i0 %o0 {0} {1} {2} {3}'
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def run(self):
        # Because of the lifecycle the drop isn't attached when the command is
        # created so we have to do it later
        json_drop = self.inputs[1]
        self._command = 'cd ; ' + CASA_COMMAND_LINE + SCRIPT_PATH + \
                        'mstransform.py %i0 %o0 {0} {1} {2}'.format(
            self._min_frequency,
            self._max_frequency,
            json_drop['Bottom edge'],
        )
        super(CasaMsTransform, self).run()

        check_measurement_set = CheckMeasurementSet(
            os.path.join(
                self.outputs[0].path,
                'vis_{0}~{1}'.format(self._min_frequency, self._max_frequency)
            )
        )
        error_message = check_measurement_set.check_tables_to_24()

        if error_message is not None:
            LOG.error(error_message)
            self.send_error_message(
                error_message,
                self.oid,
                self.uid
            )
            return 1

    def dataURL(self):
        return 'docker container chiles02:latest'


class DockerListobs(DockerApp, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._command = None
        super(DockerListobs, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerListobs, self).initialize(**kwargs)
        self._command = 'listobs.sh %i0 %o0'
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def dataURL(self):
        return 'docker container chiles02:latest'
