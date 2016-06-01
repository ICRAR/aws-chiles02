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
import json
import logging
import os
import shutil
import sqlite3

import boto3
from boto3.s3.transfer import S3Transfer

from aws_chiles02.common import run_command, ProgressPercentage
from aws_chiles02.settings_file import AWS_REGION
from dfms.drop import BarrierAppDROP, FileDROP, DirectoryContainer

LOG = logging.getLogger(__name__)


class ErrorHandling(object):
    def __init__(self):
        self._session_id = None
        self._error_message = None

    def send_error_message(self, message_text, oid, uid, queue='dfms-messages', region=AWS_REGION, profile_name='aws-chiles02'):
        self._error_message = message_text
        session = boto3.Session(profile_name=profile_name)
        sqs = session.resource('sqs', region_name=region)
        queue = sqs.get_queue_by_name(QueueName=queue)
        message = {
            'session_id': self._session_id,
            'oid': oid,
            'uid': uid,
            'message': message_text,
        }
        json_message = json.dumps(message, indent=2)
        queue.send_message(
            MessageBody=json_message,
        )

    @property
    def error_message(self):
        return self._error_message

    @property
    def session_id(self):
        return self._session_id


class CopyLogFilesApp(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        super(CopyLogFilesApp, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CopyLogFilesApp, self).initialize(**kwargs)
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def dataURL(self):
        return type(self).__name__

    def run(self):
        log_file_dir = '/mnt/dfms/dfms_root'
        s3_output = self.outputs[0]
        bucket_name = s3_output.bucket
        key = s3_output.key
        LOG.info('dir: {2}, bucket: {0}, key: {1}'.format(bucket_name, key, log_file_dir))

        # Make the tar file
        tar_filename = os.path.join(log_file_dir, 'log.tar')
        os.chdir(log_file_dir)
        bash = 'tar -cvf {0} {1}'.format(tar_filename, 'dfms*.log')
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
            return return_code

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


class CleanupDirectories(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._dry_run = None
        super(CleanupDirectories, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CleanupDirectories, self).initialize(**kwargs)
        self._session_id = self._getArg(kwargs, 'session_id', None)
        self._dry_run = self._getArg(kwargs, 'dry_run', None)

    def dataURL(self):
        return type(self).__name__

    def run(self):
        input_files = [i.path for i in self.inputs if isinstance(i, (FileDROP, DirectoryContainer))]
        LOG.info('input_files: {0}'.format(input_files))
        for input_file in input_files:
            LOG.info('Looking at {0}'.format(input_file))
            if os.path.exists(input_file):
                if os.path.isdir(input_file):
                    LOG.info('Removing directory {0}'.format(input_file))

                    def rmtree_onerror(func, path, exc_info):
                        message = 'onerror(func={0}, path={1}, exc_info={2}'.format(func, path, exc_info)
                        LOG.error(message)
                        self.send_error_message(
                            message,
                            self.oid,
                            self.uid
                        )

                    if self._dry_run:
                        LOG.info('dry_run = True')
                    else:
                        shutil.rmtree(input_file, onerror=rmtree_onerror)
                else:
                    LOG.info('Removing file {0}'.format(input_file))
                    try:
                        if self._dry_run:
                            LOG.info('dry_run = True')
                        else:
                            os.remove(input_file)
                    except OSError:
                        message = 'Cannot remove {0}'.format(input_file)
                        LOG.error(message)
                        self.send_error_message(
                            message,
                            self.oid,
                            self.uid
                        )


class InitializeSqliteApp(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._connection = None
        super(InitializeSqliteApp, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(InitializeSqliteApp, self).initialize(**kwargs)
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def dataURL(self):
        return type(self).__name__

    def run(self):
        self._connection = sqlite3.connect(self.inputs[0].path)
        self._create_tables()
        self._connection.close()

    def _create_tables(self):
        self._connection.execute('''CREATE TABLE `mstransform_times` (
`id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
`bottom_frequency`	INTEGER NOT NULL,
`top_frequency`	INTEGER NOT NULL,
`measurement_set`	TEXT NOT NULL,
`time`	REAL NOT NULL
)
''')
