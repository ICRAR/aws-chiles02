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

from aws_chiles02.apps_general import ErrorHandling
from aws_chiles02.common import run_command
from aws_chiles02.settings_file import SCRIPT_PATH, get_casa_command_line
from dlg.apps.dockerapp import DockerApp

LOG = logging.getLogger(__name__)
TAR_FILE = 'ms.tar'
logging.getLogger('boto3').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('nose').setLevel(logging.INFO)
logging.getLogger('s3transfer').setLevel(logging.INFO)


class DockerTclean(DockerApp, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._measurement_sets = None
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        self._iterations = None
        self._arcsec = None
        self._w_projection_planes = None
        self._robust = None
        self._image_size = None
        self._clean_channel_average = None
        self._produce_qa = None
        super(DockerTclean, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(DockerTclean, self).initialize(**kwargs)
        self._measurement_sets = self._getArg(kwargs, 'measurement_sets', None)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._iterations = self._getArg(kwargs, 'iterations', 10)
        self._arcsec = self._getArg(kwargs, 'arcsec', '1.25arcsec')
        self._w_projection_planes = self._getArg(kwargs, 'w_projection_planes', None)
        self._robust = self._getArg(kwargs, 'robust', None)
        self._image_size = self._getArg(kwargs, 'image_size', 2048)
        self._clean_channel_average = self._getArg(kwargs, 'clean_channel_average', '')
        self._produce_qa = self._getArg(kwargs, 'produce_qa', 'yes')
        self._command = 'tclean.sh %i0 %o0 %o0 '
        self._session_id = self._getArg(kwargs, 'session_id', None)

    def run(self):
        # Because of the lifecycle the drop isn't attached when the command is
        # created so we have to do it later
        measurement_sets = []
        for measurement_set_dir in self._measurement_sets:
            measurement_set_name = os.path.join(measurement_set_dir, 'uvsub_{0}~{1}'.format(self._min_frequency, self._max_frequency))
            if os.path.exists(measurement_set_name):
                measurement_sets.append('/dlg_root' + measurement_set_name)
            else:
                LOG.error('Missing: {0}'.format(measurement_set_name))

        if len(measurement_sets) > 0:
            self._command = 'tclean.sh %o0 {0} {1} {2} {3} {4} {5} {6} {7} {8} {9}'.format(
                self._min_frequency,
                self._max_frequency,
                self._iterations,
                self._arcsec,
                self._w_projection_planes,
                self._robust,
                self._image_size,
                self._clean_channel_average,
                self._produce_qa,
                ' '.join(measurement_sets),
            )
        else:
            LOG.error('No input files')

        super(DockerTclean, self).run()

    def dataURL(self):
        return 'docker container chiles02:latest'


class CasaTclean(DockerApp, ErrorHandling):
    def __init__(self, oid, uid, **kwargs):
        self._measurement_sets = None
        self._max_frequency = None
        self._min_frequency = None
        self._command = None
        self._iterations = None
        self._arcsec = None
        self._w_projection_planes = None
        self._robust = None
        self._image_size = None
        self._clean_channel_average = None
        self._produce_qa = None
        self._casa_version = None
        super(CasaTclean, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        super(CasaTclean, self).initialize(**kwargs)
        self._measurement_sets = self._getArg(kwargs, 'measurement_sets', None)
        self._max_frequency = self._getArg(kwargs, 'max_frequency', None)
        self._min_frequency = self._getArg(kwargs, 'min_frequency', None)
        self._iterations = self._getArg(kwargs, 'iterations', 10)
        self._arcsec = self._getArg(kwargs, 'arcsec', '1.25arcsec')
        self._w_projection_planes = self._getArg(kwargs, 'w_projection_planes', None)
        self._robust = self._getArg(kwargs, 'robust', None)
        self._image_size = self._getArg(kwargs, 'image_size', 2048)
        self._clean_channel_average = self._getArg(kwargs, 'clean_channel_average', '')
        self._produce_qa = self._getArg(kwargs, 'produce_qa', 'yes')
        self._command = 'tclean.sh %i0 %o0 %o0 '
        self._session_id = self._getArg(kwargs, 'session_id', None)
        self._casa_version = self._getArg(kwargs, 'casa_version', None)

    def run(self):
        # Because of the lifecycle the drop isn't attached when the command is
        # created so we have to do it later
        measurement_sets = []
        for measurement_set_dir in self._measurement_sets:
            measurement_set_name = os.path.join(measurement_set_dir, 'uvsub_{0}~{1}'.format(self._min_frequency, self._max_frequency))
            if os.path.exists(measurement_set_name):
                measurement_sets.append('/dlg_root' + measurement_set_name)
            else:
                LOG.error('Missing: {0}'.format(measurement_set_name))

        if len(measurement_sets) > 0:
            self._command = 'cd ; ' + get_casa_command_line(self._casa_version) + SCRIPT_PATH + \
                            'tclean.py %o0 {0} {1} {2} {3} {4} {5} {6} {7} {8} {9}'.format(
                                self._min_frequency,
                                self._max_frequency,
                                self._iterations,
                                self._arcsec,
                                self._w_projection_planes,
                                self._robust,
                                self._image_size,
                                self._clean_channel_average,
                                self._produce_qa,
                                ' '.join(measurement_sets),
                            )
            run_command(self._command)
        else:
            LOG.error('No input files')

    def dataURL(self):
        return 'CASA TClean'
