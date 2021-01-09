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
import six
from boto3.s3.transfer import S3Transfer
from dlg.apps.dockerapp import DockerApp
from dlg.drop import BarrierAppDROP

from aws_chiles02.apps_general import ErrorHandling, tag_s3_object
from aws_chiles02.common import ProgressPercentage, run_command
from aws_chiles02.settings_file import SCRIPT_PATH, get_casa_command_line

LOG = logging.getLogger(__name__)
TAR_FILE = 'ms.tar'
logging.getLogger('boto3').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('nose').setLevel(logging.INFO)
logging.getLogger('s3transfer').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

LOG.info('Python 2: {}, Python 3: {}'.format(six.PY2, six.PY3))


class CopyStatsFromS3(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._max_frequency = None
        self._min_frequency = None
        super(CopyStatsFromS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyStatsFromS3, self).initialize(**kwargs)
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

        measurement_set = os.path.join(
            measurement_set_dir,
            'uvsub_{0}~{1}'.format(self._min_frequency, self._max_frequency))
        LOG.debug('Checking {0} exists'.format(measurement_set))
        if os.path.exists(measurement_set) and os.path.isdir(measurement_set):
            LOG.warning('Measurement Set: {0} exists'.format(measurement_set))
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
        return 'CopyStatsFromS3'


class CopyStatsToS3(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        super(CopyStatsToS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyStatsToS3, self).initialize(**kwargs)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def dataURL(self):
        return 'CopyStatsToS3'

    def run(self):
        measurement_set_output = self.inputs[0]
        measurement_set_dir = measurement_set_output.path

        s3_output = self.outputs[0]
        bucket_name = s3_output.bucket
        key = s3_output.key
        LOG.info('dir: {2}, bucket: {0}, key: {1}'.format(bucket_name, key, measurement_set_dir))
        # Does the file exists
        stem_name = 'stats_{0}~{1}.csv'.format(self._min_frequency, self._max_frequency)
        msd='/'.join(measurement_set_dir.split('/')[:-1])
        file_name = os.path.join(msd,stem_name)
        LOG.debug('checking {0} exists'.format(file_name))
        if not os.path.exists(file_name) or not os.path.isfile(file_name):
            message = 'Stats: {0} does not exist'.format(file_name)
            LOG.error(message)
            self.send_error_message(
                message,
                self.oid,
                self.uid
            )
            return 1

        # Make the tar file
        tar_filename = os.path.join(
            msd,
            'stats_{0}~{1}.tar.gz'.format(self._min_frequency, self._max_frequency))
        os.chdir(msd)
        bash = 'tar -cvzf {0} {1}'.format(
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
                'StorageClass': s3_output.storage_class
            }
        )
        tag_s3_object(s3_client.get_object(Bucket=bucket_name, Key=key), s3_output.tags)

        return 0


class DockerStats(DockerApp, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._max_frequency = None
        self._min_frequency = None
        self._observation = None
        self._command = None
        super(DockerStats, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerStats, self).initialize(**kwargs)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._observation = self._getArg(kwargs, 'observation', None)
        self._command = 'stats.sh %i0 %i0 '
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def run(self):
        self._command = 'stats.sh %i0/uvsub_{0}~{1} %i0/stats_{0}~{1}.csv {2}'.format(
            self._min_frequency,
            self._max_frequency,
            self._observation,
        )
        super(DockerStats, self).run()

    def dataURL(self):
        return 'docker container chiles02:latest'


class CasaStats(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._measurement_sets = None
        self._max_frequency = None
        self._min_frequency = None
        self._observation = None
        self._command = None
        self._casa_version = None
        super(CasaStats, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CasaStats, self).initialize(**kwargs)
        self._measurement_sets = self._getArg(kwargs, 'measurement_sets', None)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._observation = self._getArg(kwargs, 'observation', None)
        self._casa_version = self._getArg(kwargs, 'casa_version', None)
        self._command = 'stats.py %i0 %i0 '
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def run(self):
        # Because of the lifecycle the drop isn't attached when the command is
        # created so we have to do it later
        measurement_sets = []
        for measurement_set_dir in self._measurement_sets:
            measurement_set_name = os.path.join(measurement_set_dir,
                                                'uvsub_{0}~{1}'.format(self._min_frequency, self._max_frequency))
            if os.path.exists(measurement_set_name):
                measurement_sets.append(measurement_set_name)
            else:
                LOG.error('Missing: {0}'.format(measurement_set_name))

        if len(measurement_sets) > 0:
            msd=self.inputs[0].path
            msd='/'.join(msd.split('/')[:-1])
            self._command = 'cd ; ' + get_casa_command_line(self._casa_version) + SCRIPT_PATH + \
                            'stats.py {4} {0}/stats_{1}~{2}.csv {3}'.format(
                                msd,
                                self._min_frequency,
                                self._max_frequency,
                                self._observation,
                                ','.join(measurement_sets)
                            )
            run_command(self._command)
        else:
            LOG.error('No input files')
    def dataURL(self):
        return 'CASA Stats'
