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

from dfms.apps.dockerapp import DockerApp
from dfms.drop import BarrierAppDROP, FileDROP, DirectoryContainer

LOG = logging.getLogger(__name__)


class DockerCopyMsTransformFromS3(DockerApp):
    def __init__(self, oid, uid, **kwargs):
        """
        initial the class, make sure super is called after the event as it calls initialize
        :param oid:
        :param uid:
        :param kwargs:
        :return:
        """
        self._aws_access_key_id = None
        self._aws_secret_access_key = None
        self._command = None
        super(DockerCopyMsTransformFromS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerCopyMsTransformFromS3, self).initialize(**kwargs)
        self._command = 'copy_mstransform_from_s3.sh %iDataURL0 %o0'

    def dataURL(self):
        return 'docker container java-s3-copy:latest'


class DockerCopyCleanFromS3(DockerApp):
    def __init__(self, oid, uid, **kwargs):
        """
        initial the class, make sure super is called after the event as it calls initialize
        :param oid:
        :param uid:
        :param kwargs:
        :return:
        """
        self._aws_access_key_id = None
        self._aws_secret_access_key = None
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        super(DockerCopyCleanFromS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerCopyCleanFromS3, self).initialize(**kwargs)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._command = 'copy_clean_from_s3.sh %iDataURL0 %o0 {0} {1}'.format(
                self._min_frequency,
                self._max_frequency,
        )

    def dataURL(self):
        return 'docker container java-s3-copy:latest'


class DockerCopyMsTransformToS3(DockerApp):
    def __init__(self, oid, uid, **kwargs):
        self._bucket = None
        self._key = None
        self._aws_access_key_id = None
        self._aws_secret_access_key = None
        self._set_name = None
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        super(DockerCopyMsTransformToS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerCopyMsTransformToS3, self).initialize(**kwargs)
        self._bucket = self._getArg(kwargs, 'bucket', None)
        self._key = self._getArg(kwargs, 'key', None)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._set_name = self._getArg(kwargs, 'set_name', None)
        self._command = 'copy_mstransform_to_s3.sh %i0 %oDataURL0 {0} {1}'.format(
                self._min_frequency,
                self._max_frequency,
        )

    def dataURL(self):
        return 'docker container java-s3-copy:latest'


class DockerCopyCleanToS3(DockerApp):
    def __init__(self, oid, uid, **kwargs):
        self._bucket = None
        self._key = None
        self._aws_access_key_id = None
        self._aws_secret_access_key = None
        self._set_name = None
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        super(DockerCopyCleanToS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerCopyCleanToS3, self).initialize(**kwargs)
        self._bucket = self._getArg(kwargs, 'bucket', None)
        self._key = self._getArg(kwargs, 'key', None)
        self._set_name = self._getArg(kwargs, 'set_name', None)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._command = 'copy_clean_to_s3.sh %i0 %oDataURL0 {0} {1}'.format(
                self._min_frequency,
                self._max_frequency,
        )

    def dataURL(self):
        return 'docker container java-s3-copy:latest'


class DockerCopyAllFromS3Folder(DockerApp):
    def __init__(self, oid, uid, **kwargs):
        self._bucket = None
        self._key = None
        self._aws_access_key_id = None
        self._aws_secret_access_key = None
        self._set_name = None
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        super(DockerCopyAllFromS3Folder, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerCopyAllFromS3Folder, self).initialize(**kwargs)
        self._bucket = self._getArg(kwargs, 'bucket', None)
        self._key = self._getArg(kwargs, 'key', None)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._set_name = self._getArg(kwargs, 'set_name', None)
        self._command = 'copy_from_s3_all.sh %oDataURL0 {0} {1}'.format(
                self._min_frequency,
                self._max_frequency,
        )

    def dataURL(self):
        return 'docker container java-s3-copy:latest'


class DockerMsTransform(DockerApp):
    def __init__(self, oid, uid, **kwargs):
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        super(DockerMsTransform, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerMsTransform, self).initialize(**kwargs)

        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._command = 'mstransform.sh %i0 %o0 %o0 {0} {1} {2}'

    def run(self):
        # Because of the lifecycle the drop isn't attached when the command is
        # created so we have to do it later
        json_drop = self.inputs[1]
        self._command = 'mstransform.sh %i0 %o0 %o0 {0} {1} {2}'.format(
                self._min_frequency,
                self._max_frequency,
                json_drop['Bottom edge']
        )
        super(DockerMsTransform, self).run()

    def dataURL(self):
        return 'docker container chiles02:latest'


class DockerClean(DockerApp):
    def __init__(self, oid, uid, **kwargs):
        self._measurement_sets = None
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        self._iterations = None
        super(DockerClean, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerClean, self).initialize(**kwargs)

        self._measurement_sets = self._getArg(kwargs, 'measurement_sets', None)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._iterations = self._getArg(kwargs, 'iterations', 10)
        self._command = 'clean.sh %i0 %o0 %o0 '

    def run(self):
        # Because of the lifecycle the drop isn't attached when the command is
        # created so we have to do it later
        measurement_sets = ['/dfms_root' + os.path.join(i, 'vis_{0}~{1}'.format(self._min_frequency, self._max_frequency)) for i in self._measurement_sets]
        self._command = 'clean.sh %o0 {0} {1} {2} {3}'.format(
                self._min_frequency,
                self._max_frequency,
                self._iterations,
                ' '.join(measurement_sets),
        )
        super(DockerClean, self).run()

    def dataURL(self):
        return 'docker container chiles02:latest'


class DockerListobs(DockerApp):
    def __init__(self, oid, uid, **kwargs):
        self._command = None
        super(DockerListobs, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerListobs, self).initialize(**kwargs)
        self._command = 'listobs.sh %i0 %o0'

    def dataURL(self):
        return 'docker container chiles02:latest'


class CleanupDirectories(BarrierAppDROP):
    def __init__(self, oid, uid, **kwargs):
        super(CleanupDirectories, self).__init__(oid, uid, **kwargs)

    def dataURL(self):
        return type(self).__name__

    def run(self):
        input_files = [i for i in self.inputs if isinstance(i, (FileDROP, DirectoryContainer))]
        for input_file in input_files:
            if os.path.exists(input_file):
                if os.path.isdir(input_file):
                    LOG.info('Removing directory {0}'.format(input_file))
                    shutil.rmtree(input_file, ignore_errors=True)
                else:
                    LOG.info('Removing file {0}'.format(input_file))
                    os.remove(input_file)


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
