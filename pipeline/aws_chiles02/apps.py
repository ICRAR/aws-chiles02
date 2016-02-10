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
import subprocess
import time
import sqlite3

from dfms.apps.dockerapp import DockerApp
from dfms.drop import BarrierAppDROP, FileDROP, DirectoryContainer
from dfms import utils

LOG = logging.getLogger(__name__)


class DockerCopyFromS3(DockerApp):
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
        super(DockerCopyFromS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerCopyFromS3, self).initialize(**kwargs)
        self._command = 'copy_from_s3.sh %iDataURL0 %o0'

    def dataURL(self):
        return 'sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/java-s3-copy:latest'


class DockerCopyToS3(DockerApp):
    def __init__(self, oid, uid, **kwargs):
        self._bucket = None
        self._key = None
        self._aws_access_key_id = None
        self._aws_secret_access_key = None
        self._set_name = None
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        super(DockerCopyToS3, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerCopyToS3, self).initialize(**kwargs)
        self._bucket = self._getArg(kwargs, 'bucket', None)
        self._key = self._getArg(kwargs, 'key', None)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._set_name = self._getArg(kwargs, 'set_name', None)
        self._command = 'copy_to_s3.sh %i0 %oDataURL0 {0} {1}'.format(
                self._min_frequency,
                self._max_frequency,
        )

    def dataURL(self):
        return 'sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/java-s3-copy:latest'


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
        return 'sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/java-s3-copy:latest'


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
        return 'sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/chiles02:latest'


class DockerClean(DockerApp):
    def __init__(self, oid, uid, **kwargs):
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        super(DockerClean, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerClean, self).initialize(**kwargs)

        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._command = 'clean.sh %i0 %o0 %o0 {0} {1} {2}'

    def run(self):
        # Because of the lifecycle the drop isn't attached when the command is
        # created so we have to do it later
        json_drop = self.inputs[1]
        self._command = 'mstransform.sh %i0 %o0 %o0 {0} {1} {2}'.format(
                self._min_frequency,
                self._max_frequency,
                json_drop['Bottom edge']
        )
        super(DockerClean, self).run()

    def dataURL(self):
        return 'sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/chiles02:latest'


class DockerListobs(DockerApp):
    def __init__(self, oid, uid, **kwargs):
        self._command = None
        super(DockerListobs, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerListobs, self).initialize(**kwargs)
        self._command = 'listobs.sh %i0 %o0'

    def dataURL(self):
        return 'sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/chiles02:latest'


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


class BashShellApp(BarrierAppDROP):
    def __init__(self, oid, uid, **kwargs):
        self._command = None
        self._exit_code = None
        super(BashShellApp, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        BarrierAppDROP.initialize(self, **kwargs)

        self._command = self._getArg(kwargs, 'command', None)
        if not self._command:
            raise Exception('No command specified, cannot create BashShellApp')

    def run(self):
        inputs = [i.path for i in self.inputs if isinstance(i, (FileDROP, DirectoryContainer))]
        outputs = [o.path for o in self.outputs if isinstance(o, (FileDROP, DirectoryContainer))]

        # Replace any input/output placeholders that might be found in the
        # command line by the real path of the inputs and outputs
        cmd = self._command
        for x, i in enumerate(inputs):
            cmd = cmd.replace('%i{0}'.format(x), i)
        for x, o in enumerate(outputs):
            cmd = cmd.replace('%o{0}'.format(x), o)

        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Command after binding placeholder replacement is: {0}".format(cmd))

        # Inputs/outputs that are not FileDROPs or DirectoryContainers can't
        # bind their data via volumes into the docker container. Instead they
        # communicate their dataURL via command-line replacement
        input_data_urls = [i.dataURL for i in self.inputs if not isinstance(i, (FileDROP, DirectoryContainer))]
        output_data_urls = [o.dataURL for o in self.outputs if not isinstance(o, (FileDROP, DirectoryContainer))]

        for x, i in enumerate(input_data_urls):
            cmd = cmd.replace("%iDataURL{0}".format(x), i)
        for x, o in enumerate(output_data_urls):
            cmd = cmd.replace("%oDataURL{0}".format(x), o)

        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Command after data URL placeholder replacement is: {0}".format(cmd))

        # Wrap everything inside bash
        cmd = '/bin/bash -c "{0}"'.format(utils.escapeQuotes(cmd, singleQuotes=False))

        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Command after user creation and wrapping is: {0}".format(cmd))

        start = time.time()

        # Wait until it finishes
        process = subprocess.Popen(cmd, bufsize=1, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ.copy())
        stdout, stderr = process.communicate()
        self._exit_code = process.returncode
        end = time.time()
        if LOG.isEnabledFor(logging.INFO):
            LOG.info("Finished in {0:.2f} [s] with exit code {1}".format(end-start, self._exit_code))

        if self._exit_code == 0 and LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Command finished successfully, output follows.\n==STDOUT==\n{0}==STDERR==\n{1}".format(stdout, stderr))
        elif self._exit_code != 0:
            message = "Command didn't finish successfully (exit code {0})".format(self._exit_code)
            LOG.error(message + ", output follows.\n==STDOUT==\n%s==STDERR==\n%s" % (stdout, stderr))
            raise Exception(message)

    def dataURL(self):
        return type(self).__name__
