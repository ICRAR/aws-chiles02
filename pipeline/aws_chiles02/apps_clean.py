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

from dfms.apps.dockerapp import DockerApp

LOG = logging.getLogger(__name__)


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
