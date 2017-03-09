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
Load all the files and spit out a single file
"""
import argparse
import csv
import glob
import os
import tarfile
import logging

from astropy.time import Time

LOG = logging.getLogger(__name__)


class ReadFiles(object):
    def __init__(self, data_dir_name, output_file_name, frequencies):
        self._data_dir_name = data_dir_name
        self._output_file_name = output_file_name
        self._frequencies = frequencies

        self._map_observation = {}
        self._write_file = None
        self._csv_writer = None

    def _get_observation_id(self, observation_name):
        observation_id = self._map_observation.get(observation_name)
        if observation_id is None:
            elements = observation_name.split('.')
            observation_id = elements[3] + '.' + elements[4]
            self._map_observation[observation_name] = observation_id

        return observation_id

    def merge_csv_files(self):
        self._write_file = open(self._output_file_name, 'w')
        self._csv_writer = csv.writer(self._write_file, quoting=csv.QUOTE_MINIMAL)
        self._csv_writer.writerow([
            'observation_id',
            'scan',
            'begin_time',
            'end_time',
            'channel',
            'max',
            'mean',
            'medabsdevmed',
            'median',
            'min',
            'npts',
            'quartile',
            'rms',
            'stddev',
            'sum',
            'sumsq',
            'var',
            'minimum_frequency',
            'maximum_frequency',
            'hour_angle'
        ])

        # Loop through all the files
        for file_name in glob.glob(os.path.join(self._data_dir_name, '*/stats_13B*.tar.gz')):
            LOG.info('Reading {0}'.format(file_name))
            with tarfile.open(file_name, "r:gz") as tar:
                for tar_item in tar.getnames():
                    # Get the minimum and maximum frequency from the filename
                    (basename, ext) = os.path.splitext(tar_item)
                    elements = basename.split('_')
                    elements = elements[1].split('~')
                    minimum_frequency = int(elements[0])
                    maximum_frequency = int(elements[1])

                    if self._frequencies_required(minimum_frequency, maximum_frequency):
                        read_file = tar.extractfile(tar_item)
                        csv_reader = csv.reader(read_file)
                        for row in csv_reader:
                            # Ignore the header row
                            if csv_reader.line_num > 1:
                                new_data = self._update_data(row, minimum_frequency, maximum_frequency)
                                self._csv_writer.writerow(new_data)

        self._write_file.close()

    def _update_data(self, row, minimum_frequency, maximum_frequency):
        return [
            self._get_observation_id(row[0]),   # observation_id
            row[1],                             # scan
            row[2],                             # begin_time
            row[3],                             # end_time
            row[5],                             # channel
            self._reduce(row[6]),               # max
            self._reduce(row[7]),               # mean
            self._reduce(row[8]),               # medabsdevmed
            self._reduce(row[9]),               # median
            self._reduce(row[10]),              # min
            row[11],                            # npts
            self._reduce(row[12]),              # quartile
            self._reduce(row[13]),              # rms
            self._reduce(row[14]),              # stddev
            self._reduce(row[15]),              # sum
            self._reduce(row[16]),              # sumsq
            self._reduce(row[17]),              # var
            minimum_frequency,                  # minimum_frequency
            maximum_frequency,                  # maximum_frequency
            self._hour_angle(row[2], row[3])
        ]

    @staticmethod
    def _reduce(data):
        if type(data) == float:
            return '{0:.5f}'.format(data)

        if type(data) == str and data.index('.') > 0:
            elements = data.split('.')
            if len(elements) == 2:
                return elements[0] + '.' + elements[1][0:5]

        return data

    def _frequencies_required(self, minimum_frequency, maximum_frequency):
        if self._frequencies is None:
            return True

        for pair in self._frequencies:
            if minimum_frequency >= pair[0] and maximum_frequency <= pair[1]:
                return True

        return False

    @staticmethod
    def _hour_angle(start, end):
        modified_julian_day = float(end) + float(start) / 2.0
        time = Time(modified_julian_day, format='mjd')

        c = coord.ICRSCoordinates(ra=ra, dec = dec, unit=(u.radian, u.radian))
        ha = coord.angles.RA.hour_angle(c.ra, t)
        return ha


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('data_dir_name', nargs=1, help='the root directory to use')
    parser.add_argument('output_file_name', nargs=1, help='the output file name')
    parser.add_argument('--frequencies', nargs='*', help='the frequencies to process')
    args = parser.parse_args()

    frequencies = None
    if args.frequencies is not None:
        frequencies = []
        for pair in args.frequencies:
            elements = pair.split('-')
            minimum_frequency = int(elements[0])
            maximum_frequency = int(elements[1])
            frequencies.append((minimum_frequency, maximum_frequency))

    read_files = ReadFiles(args.data_dir_name[0], args.output_file_name[0], frequencies)
    read_files.merge_csv_files()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
