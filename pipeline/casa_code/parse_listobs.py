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
A class to parse the list obs output
"""
import json


class ParseListobs:
    def __init__(self, filename):
        self._filename = filename
        self._data = {}

    def parse(self):
        # Load all the lines
        with open(self._filename, mode='r') as read_file:
            lines = read_file.readlines()

        # Process the lines
        mode = None
        fields = {}
        spectral_windows = {}
        for line in lines:
            line = line.strip()
            if len(line) == 0:
                pass
            elif line.startswith('MeasurementSet Name:'):
                elements = line.split()
                self._data['MeasurementSet Name'] = elements[2]
                mode = None
            elif line.startswith('Observer:'):
                elements = line.split()
                self._data['Observer'] = ' '.join(elements[1:-2])
                self._data['Project'] = elements[-1]
                mode = None
            elif line.startswith('Observation:'):
                elements = line.split()
                self._data['Observation'] = ' '.join(elements[1:])
                mode = None
            elif line.startswith('Data records:'):
                elements = line.split()
                self._data['Data records'] = elements[2]
                self._data['Total integration time'] = ' '.join(elements[-2:])
                mode = 'Data records'
            elif line.startswith('Fields:'):
                elements = line.split()
                fields['Field count'] = elements[1]
                fields['Fields'] = []
                self._data['Fields'] = fields
                mode = 'Fields'
            elif line.startswith('Spectral Windows:'):
                elements = line.split()
                spectral_windows['Spectral Windows details'] = ' '.join(elements[2:])
                spectral_windows['Spectral Windows'] = []
                self._data['Spectral Windows'] = spectral_windows
                mode = 'Spectral Windows'
            elif line.startswith('Antennas:'):
                mode = 'Antennas'

            elif mode == 'Data records':
                elements = line.split()
                self._data['Observed from'] = elements[2]
                self._data['Observed to'] = elements[4]
            elif mode == 'Fields':
                if not line.startswith('ID'):
                    elements = line.split()
                    fields_list = fields['Fields']
                    fields_list.append(
                            [elements[0],
                             elements[1],
                             elements[2],
                             elements[3],
                             elements[4],
                             elements[5],
                             elements[6],
                             elements[7],
                             ])
            elif mode == 'Spectral Windows':
                if not line.startswith('SpwID'):
                    elements = line.split()
                    spectral_windows_list = spectral_windows['Spectral Windows']
                    spectral_windows_list.append(
                            [elements[0],
                             elements[1],
                             elements[2],
                             elements[3],
                             elements[4],
                             elements[5],
                             elements[6],
                             elements[7],
                             elements[8],
                             elements[9],
                             ])
                    if elements[0] == '0':
                        self._data['Bottom edge'] = elements[4]

    def get_json_string(self):
        return json.dumps(self._data, indent=2)

    def get_data(self):
        return self._data
