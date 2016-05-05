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
Perform checks on the measurement set
"""
import logging
import os

LOG = logging.getLogger(__name__)

EXT_TO_24 = [
    '.dat',
    '.f1',
    '.f2',
    '.f3',
    '.f4',
    '.f5',
    '.f6',
    '.f7',
    '.f8',
    '.f9',
    '.f10',
    '.f11',
    '.f12',
    '.f13',
    '.f14',
    '.f15',
    '.f16',
    '.f17',
    '.f17_TSM1',
    '.f18',
    '.f19',
    '.f20',
    '.f20_TSM0',
    '.f21',
    '.f21_TSM1',
    '.f22',
    '.f22_TSM1',
    '.f23',
    '.f23_TSM1',
    '.f24',
    '.f24_TSM1',
]
EXT_TO_26 = [
    '.dat',
    '.f1',
    '.f2',
    '.f3',
    '.f4',
    '.f5',
    '.f6',
    '.f7',
    '.f8',
    '.f9',
    '.f10',
    '.f11',
    '.f12',
    '.f13',
    '.f14',
    '.f15',
    '.f16',
    '.f17',
    '.f17_TSM1',
    '.f18',
    '.f19',
    '.f20',
    '.f20_TSM0',
    '.f21',
    '.f21_TSM1',
    '.f22',
    '.f22_TSM1',
    '.f23',
    '.f23_TSM1',
    '.f24',
    '.f24_TSM1',
    '.f25',
    '.f25_TSM1',
    '.f26',
    '.f26_TSM1',
]


class CheckMeasurementSet:
    def __init__(self, measurement_set):
        self._measurement_set = measurement_set

    def check_tables_to_23(self):
        return self._check_tables(EXT_TO_24)

    def check_tables_to_26(self):
        return self._check_tables(EXT_TO_26)

    def _check_tables(self, extension_list):
        LOG.info('Measurement Set: {0}'.format(self._measurement_set))
        if os.path.exists(self._measurement_set):
            # Take a copy of the list
            to_find = list(extension_list)

            for filename in os.listdir(self._measurement_set):
                full_pathname = os.path.join(self._measurement_set, filename)
                LOG.info('filename: {0}, full_pathname: {1}'.format(filename, full_pathname))
                if os.path.isfile(full_pathname):
                    filename_stub, file_extension = os.path.splitext(filename)
                    LOG.info('filename_stub: {0}, file_extension: {1}'.format(filename_stub, file_extension))

                    if filename_stub == 'table' and file_extension in to_find:
                        LOG.info('Found {0}{1}'.format(filename_stub, file_extension))
                        to_find.remove(file_extension)

                    if len(to_find) == 0:
                        break

            if len(to_find) > 0:
                return 'The following extensions are missing\n  {0}'.format(' '.join(to_find))
        else:
            LOG.warning('Measurement Set: {0} does not exist'.format(self._measurement_set))

        return None
