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
import sqlite3
import threading

from dfms.apps.dockerapp import DockerApp
from dfms.drop import BarrierAppDROP, FileDROP, DirectoryContainer

LOG = logging.getLogger(__name__)


class DockerCopyCasaPyLogsToS3(DockerApp):
    def __init__(self, oid, uid, **kwargs):
        self._bucket = None
        self._key = None
        self._aws_access_key_id = None
        self._aws_secret_access_key = None
        self._command = None
        super(DockerCopyCasaPyLogsToS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerCopyCasaPyLogsToS3, self).initialize(**kwargs)
        self._bucket = self._getArg(kwargs, 'bucket', None)
        self._key = self._getArg(kwargs, 'key', None)
        self._command = 'copy_casapy_logs_to_s3.sh %oDataURL0 %i0'

    def dataURL(self):
        return 'docker container java-s3-copy:latest'


class CleanupDirectories(BarrierAppDROP):
    def __init__(self, oid, uid, **kwargs):
        super(CleanupDirectories, self).__init__(oid, uid, **kwargs)

    def dataURL(self):
        return type(self).__name__

    def run(self):
        input_files = [i.path for i in self.inputs if isinstance(i, (FileDROP, DirectoryContainer))]
        for input_file in input_files:
            if os.path.exists(input_file):
                if os.path.isdir(input_file):
                    LOG.info('Removing directory {0}'.format(input_file))
                    shutil.rmtree(input_file, ignore_errors=True)
                else:
                    LOG.info('Removing file {0}'.format(input_file))
                    try:
                        os.remove(input_file)
                    except OSError:
                        LOG.exception('Cannot remove {0}'.format(input_file))


class InitializeSqliteApp(BarrierAppDROP):
    def __init__(self, oid, uid, **kwargs):
        self._connection = None
        super(InitializeSqliteApp, self).__init__(oid, uid, **kwargs)

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


class ProgressPercentage:
    def __init__(self, filename, expected_size):
        self._filename = filename
        self._size = float(expected_size)
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._percentage = -1

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = int((self._seen_so_far / self._size) * 100.0)
            if percentage > self._percentage:
                LOG.info(
                        '{0}  {1} / {2}  ({3:.2f}%)'.format(
                            self._filename,
                            self._seen_so_far,
                            self._size,
                            percentage))
                self._percentage = percentage
