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
Check the splits
"""
import logging

import argparse
import boto3
import collections

from aws_chiles02.common import get_list_frequency_groups, FrequencyPair
from aws_chiles02.generate_mstransform_graph import MeasurementSetData

LOG = logging.getLogger(__name__)


def get_measurement_sets(bucket):
    list_measurement_sets = []
    for key in bucket.objects.filter(Prefix='observation_data'):
        if key.key.endswith('_calibrated_deepfield.ms.tar'):
            elements = key.key.split('/')
            if len(elements) >= 2:
                list_measurement_sets.append(MeasurementSetData(elements[1], key.size))
    return list_measurement_sets


def parse_arguments():
    parser = argparse.ArgumentParser('Check what data has been split')
    parser.add_argument('bucket', help='the bucket to access')
    parser.add_argument('-w', '--width', type=int, help='the frequency width', default=4)
    return parser.parse_args()


def get_split(bucket, width):
    split_data = []
    for key in bucket.objects.filter(Prefix='split_{0}'.format(width)):
        elements = key.key.split('/')
        if len(elements) > 2:
            split_data.append([elements[2][:-4], elements[1]])

    return split_data


def analyse_data(measurement_sets, split_entries, width):
    # Build the expected list
    list_frequencies = get_list_frequency_groups(width)

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
    for key, value in ordered_dictionary.iteritems():
        if len(value) == number_entries:
            output1 += '{0} = "All"\n'.format(key)
            output2 += '{0} '.format(key)
        else:
            output1 += '{0} = "{1}"\n'.format(key, value)
            if len(value) >= 1:
                output2 += '{0} '.format(key)

    LOG.info(output1)
    LOG.info(output2)

    return ordered_dictionary


def main():
    arguments = parse_arguments()
    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)
    bucket = s3.Bucket(arguments.bucket)

    # Get the data we need
    measurement_sets = get_measurement_sets(bucket)
    split_entries = get_split(bucket, arguments.width)

    analyse_data(measurement_sets, split_entries, arguments.width)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
