#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved (c) 2019
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
Check the splits
"""
import argparse
import collections
import logging
from os.path import exists, split

import boto3

from aws_chiles02.common import FrequencyPair, get_list_frequency_groups, get_config
from aws_chiles02.generate_mstransform_graph import MeasurementSetData

LOGGER = logging.getLogger(__name__)


def get_measurement_sets(bucket):
    list_measurement_sets = []
    for key in bucket.objects.filter(Prefix='observation_data/phase_1'):
        if key.key.endswith('_calibrated_deepfield.ms.tar.gz'):
            elements = key.key.split('/')
            if len(elements) >= 2:
                list_measurement_sets.append(MeasurementSetData(elements[-1], key.size, True))
    return list_measurement_sets


def get_split(bucket, **kwargs):
    split_data = []
    for key in bucket.objects.filter(Prefix=kwargs['split_directory']):
        elements = key.key.split('/')
        if elements[1] == 'logs' or elements[1].endswith('yaml'):
            pass
        elif len(elements) > 2:
            split_data.append([elements[2][:-4], elements[1]])

    return split_data


def analyse_data(measurement_sets, split_entries, **kwargs):
    # Build the expected list
    list_frequencies = get_list_frequency_groups(kwargs['width'])

    expected_combinations = {}
    for key in measurement_sets:
        list_data = []
        for frequency in list_frequencies:
            list_data.append(frequency.name)
        expected_combinations[key.short_name] = list_data

    for element in split_entries:
        frequencies = expected_combinations[element[0]]
        pair = element[1].split('_')
        frequency_pair = FrequencyPair(pair[0], pair[1]).name
        if frequency_pair in frequencies:
            frequencies.remove(frequency_pair)

    number_entries = len(list_frequencies)
    ordered_dictionary = collections.OrderedDict(sorted(expected_combinations.items()))
    output1 = '\n'
    output2 = '\n'
    for key, value in ordered_dictionary.items():
        if len(value) == number_entries:
            output1 += '{0} = "All"\n'.format(key)
            output2 += '{0}\n'.format(key)
        else:
            output1 += '{0} = "{1}"\n'.format(key, value)
            if len(value) >= 1:
                output2 += '{0}\n'.format(key)

    LOGGER.info(output1)
    LOGGER.info(output2)

    return ordered_dictionary


def main(command_line_):
    if command_line_.config_file is not None:
        if exists(command_line_.config_file):
            yaml_filename = command_line_
        else:
            LOGGER.error('Invalid configuration filename: {}'.format(command_line_.config_file))
            return
    else:
        path_dirname, filename = split(__file__)
        yaml_filename = '{0}/aws-chiles02.yaml'.format(path_dirname)

    LOGGER.info('Reading YAML file {}'.format(yaml_filename))
    config = get_config(yaml_filename, 'split')

    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)
    bucket = s3.Bucket(config['bucket_name'])

    # Get the data we need
    measurement_sets = get_measurement_sets(bucket)
    split_entries = get_split(bucket, **config)

    analyse_data(measurement_sets, split_entries, **config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Check Splits')
    parser.add_argument(
        '--config_file',
        default=None,
        help='the config file for this run'
    )
    command_line = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    main(command_line)
