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
from dfms.apps.dockerapp import DockerApp


class DockerCopyFromS3(DockerApp):
    def __init__(self, oid, uid, **kwargs):
        super(DockerApp, self).__init__(oid, uid, **kwargs)
        self._bucket = None
        self._key = None
        self._aws_access_key_id = None
        self._aws_secret_access_key = None
        self._s3 = None
        self._command = None

    def initialize(self, **kwargs):
        """

        :param kwargs: the dictionary of arguments
        """
        super(DockerApp, self).initialize(**kwargs)
        self._bucket = self._getArg(kwargs, 'bucket', None)
        self._key = self._getArg(kwargs, 'key', None)
        self._aws_access_key_id = self._getArg(kwargs, 'aws_access_key_id', None)
        self._aws_secret_access_key = self._getArg(kwargs, 'aws_secret_access_key', None)

    def run(self):
        self._command = 'copy_from_s3_mt.sh %iDataURL0 %o0 {0} {1}'.format(self._aws_access_key_id, self._aws_secret_access_key)
        super(DockerApp, self).run()

    def dataURL(self):
        return 'sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/java-s3-copy:latest'


class DockerCopyToS3(DockerApp):
    def __init__(self, oid, uid, **kwargs):
        super(DockerApp, self).__init__(oid, uid, **kwargs)
        self._bucket = None
        self._key = None
        self._aws_access_key_id = None
        self._aws_secret_access_key = None
        self._s3 = None
        self._command = None

    def initialize(self, **kwargs):
        """

        :param kwargs: the dictionary of arguments
        """
        super(DockerApp, self).initialize(**kwargs)
        self._bucket = self._getArg(kwargs, 'bucket', None)
        self._key = self._getArg(kwargs, 'key', None)
        self._aws_access_key_id = self._getArg(kwargs, 'aws_access_key_id', None)
        self._aws_secret_access_key = self._getArg(kwargs, 'aws_secret_access_key', None)

    def run(self):
        self._command = 'copy_to_s3.sh %i0 %oDataURL0 {0} {1}'.format(self._aws_access_key_id, self._aws_secret_access_key)
        super(DockerApp, self).run()

    def dataURL(self):
        return 'sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/java-s3-copy:latest'


class DockerMsTransform(DockerApp):
    def __init__(self, oid, uid, **kwargs):
        super(DockerApp, self).__init__(oid, uid, **kwargs)
        self._max_frequency = None
        self._min_frequency = None
        self._command = None

    def initialize(self, **kwargs):
        """

        :param kwargs: the dictionary of arguments
        """
        super(DockerApp, self).initialize(**kwargs)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)

    def run(self):
        self._command = 'mstransform.sh %i0 %o0 %o0 {0} {1}'.format(self._min_frequency, self._max_frequency)
        super(DockerApp, self).run()

    def dataURL(self):
        return 'sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/chiles02:latest'
