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
import shutil

import boto3
import os
from boto3.s3.transfer import S3Transfer

from aws_chiles02.apps_general import ErrorHandling
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


class CopyUvsubFromS3(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._max_frequency = None
        self._min_frequency = None
        super(CopyUvsubFromS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyUvsubFromS3, self).initialize(**kwargs)
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

        measurement_set = os.path.join(measurement_set_dir, 'vis_{0}~{1}'.format(self._min_frequency, self._max_frequency))
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
        return 'CopyUvsubFromS3'


class CopyUvsubToS3(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        super(CopyUvsubToS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyUvsubToS3, self).initialize(**kwargs)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def dataURL(self):
        return 'CopyUvSubToS3'

    def run(self):
        measurement_set_output = self.inputs[0]
        measurement_set_dir = measurement_set_output.path

        s3_output = self.outputs[0]
        bucket_name = s3_output.bucket
        key = s3_output.key
        LOG.info('dir: {2}, bucket: {0}, key: {1}'.format(bucket_name, key, measurement_set_dir))
        # Does the file exists
        stem_name = 'uvsub_{0}~{1}'.format(self._min_frequency, self._max_frequency)
        measurement_set = os.path.join(measurement_set_dir, stem_name)
        LOG.debug('checking {0} exists'.format(measurement_set))
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
        tar_filename = os.path.join(measurement_set_dir, 'uvsub_{0}~{1}.tar'.format(self._min_frequency, self._max_frequency))
        os.chdir(measurement_set_dir)
        bash = 'tar -cvf {0} {1}'.format(
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


class CopyPngsToS3(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        super(CopyPngsToS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyPngsToS3, self).initialize(**kwargs)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def dataURL(self):
        return 'CopyPngsToS3'

    def run(self):
        png_output = self.inputs[0]
        png_output_dir = png_output.path

        s3_output = self.outputs[0]
        bucket_name = s3_output.bucket
        key = s3_output.key
        LOG.info('dir: {2}, bucket: {0}, key: {1}'.format(bucket_name, key, png_output_dir))
        # Does the file exists
        stem_name = 'qa_pngs'.format(self._min_frequency, self._max_frequency)
        png_directory = os.path.join(png_output_dir, stem_name)
        LOG.debug('checking {0} exists'.format(png_directory))
        if not os.path.exists(png_directory) or not os.path.isdir(png_directory):
            message = 'PNG Directory: {0} does not exist'.format(png_directory)
            LOG.error(message)
            self.send_error_message(
                message,
                self.oid,
                self.uid
            )
            return 0

        # Make the tar file
        tar_filename = os.path.join(png_output_dir, 'pngs_{0}~{1}.tar'.format(self._min_frequency, self._max_frequency))
        os.chdir(png_output_dir)
        bash = 'tar -cvf {0} {1}'.format(
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


class CopyModel(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        super(CopyModel, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyModel, self).initialize(**kwargs)
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def dataURL(self):
        return 'CopyModel'

    def run(self):
        root_directory = '/home/ec2-user/aws-chiles02/LSM'
        output_directory = os.path.join(self.outputs[0].path, 'LSM')

        LOG.info('Model copy: {}, {}'.format(root_directory, self.outputs[0].path))
        shutil.copytree(root_directory, output_directory, symlinks=True)


class DockerUvsub(DockerApp, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._max_frequency = None
        self._min_frequency = None
        self._w_projection_planes = None
        self._number_taylor_terms = None
        self._command = None
        super(DockerUvsub, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerUvsub, self).initialize(**kwargs)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._w_projection_planes = self._getArg(kwargs, 'w_projection_planes', None)
        self._number_taylor_terms = self._getArg(kwargs, 'number_taylor_terms', None)
        self._command = 'uvsub.sh'
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def run(self):
        measurement_set_in = os.path.join(
            self.inputs[0].path,
            'vis_{0}~{1}'.format(self._min_frequency, self._max_frequency)
        )

        spectral_window = int(((int(self._min_frequency) + int(self._max_frequency)) / 2 - 946) / 32)
        self._command = 'uvsub_ha.sh /dlg_root{0} /dlg_root{1} {2} {3} {4} {5} ' \
                        '/opt/chiles02/aws-chiles02/LSM/epoch1gt4k_si_spw_{6}.model.tt0 ' \
                        '/opt/chiles02/aws-chiles02/LSM/epoch1gt4k_si_spw_{6}.model.tt1 '  \
                        '/opt/chiles02/aws-chiles02/LSM/Outliers/Outlier_1.0,8.spw_{6}.model '  \
                        '/opt/chiles02/aws-chiles02/LSM/Outliers/Outlier_2.0,8.spw_{6}.model '  \
                        '/opt/chiles02/aws-chiles02/LSM/Outliers/Outlier_3.0,8.spw_{6}.model '  \
                        '/opt/chiles02/aws-chiles02/LSM/Outliers/Outlier_4.0,8.spw_{6}.model '  \
                        '/opt/chiles02/aws-chiles02/LSM/Outliers/Outlier_5.0,8.spw_{6}.model '  \
                        '/opt/chiles02/aws-chiles02/LSM/Outliers/Outlier_6.0,8.spw_{6}.model '.format(
                            measurement_set_in,
                            self.outputs[0].path,
                            'uvsub_{0}~{1}'.format(self._min_frequency, self._max_frequency),
                            'qa_pngs',
                            self._w_projection_planes,
                            self._number_taylor_terms,
                            spectral_window,
                        )
        super(DockerUvsub, self).run()

    def dataURL(self):
        return 'docker container chiles02:latest'


class CasaUvsub(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._max_frequency = None
        self._min_frequency = None
        self._w_projection_planes = None
        self._number_taylor_terms = None
        self._copy_of_model = None
        self._command = None
        super(CasaUvsub, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CasaUvsub, self).initialize(**kwargs)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._w_projection_planes = self._getArg(kwargs, 'w_projection_planes', None)
        self._number_taylor_terms = self._getArg(kwargs, 'number_taylor_terms', None)
        self._command = 'uvsub.py'
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def run(self):
        # make the input measurement set
        measurement_set_in = os.path.join(
            self.inputs[0].path,
            'vis_{0}~{1}'.format(self._min_frequency, self._max_frequency)
        )
        copy_of_model = self.inputs[1].path

        spectral_window = int(((int(self._min_frequency) + int(self._max_frequency)) / 2 - 946) / 32)
        self._command = 'cd ; ' + CASA_COMMAND_LINE + SCRIPT_PATH + \
                        'uvsub_ha.py {0} {1} {2} {3} {4} {5} ' \
                        '{6}/LSM/epoch1gt4k_si_spw_{7}.model.tt0 ' \
                        '{6}/LSM/epoch1gt4k_si_spw_{7}.model.tt1 ' \
                        '{6}/LSM/Outliers/Outlier_1.0,8.spw_{7}.model ' \
                        '{6}/LSM/Outliers/Outlier_2.0,8.spw_{7}.model ' \
                        '{6}/LSM/Outliers/Outlier_3.0,8.spw_{7}.model ' \
                        '{6}/LSM/Outliers/Outlier_4.0,8.spw_{7}.model ' \
                        '{6}/LSM/Outliers/Outlier_5.0,8.spw_{7}.model ' \
                        '{6}/LSM/Outliers/Outlier_6.0,8.spw_{7}.model '.format(
                            measurement_set_in,
                            self.outputs[0].path,
                            'uvsub_{0}~{1}'.format(self._min_frequency, self._max_frequency),
                            'qa_pngs',
                            self._w_projection_planes,
                            self._number_taylor_terms,
                            copy_of_model,
                            spectral_window,
                        )
        run_command(self._command)

    def dataURL(self):
        return 'CASA UvSub'
