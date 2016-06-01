#
#    Copyright (c) UWA, The University of Western Australia
#    M468/35 Stirling Hwy
#    Perth WA 6009
#    Australia
#
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
Check the CVEL output
"""
import logging
import argparse
import boto3

from aws_chiles02.common import get_list_frequency_groups

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)


def get_clean(bucket, width, iterations):
    clean_data = []
    for key in bucket.objects.filter(Prefix='clean_{0}_{1}'.format(width, iterations)):
        elements = key.key.split('/')
        if len(elements) >= 2:
            if len(elements[1]) > 0:
                clean_data.append(elements[1])

    return clean_data


def analyse_data(clean_entries, width):
    # Build the expected list
    expected_combinations = [
        'cleaned_{0}_{1}.tar'.format(frequency.bottom_frequency, frequency.top_frequency) for frequency in get_list_frequency_groups(width) ]

    output = '\n'
    list_output = '\n'
    for key in sorted(expected_combinations):
        output += '{0} = {1}\n'.format(key, 'Done' if clean_entries.__contains__(key) else 'Not done')
        if not clean_entries.__contains__(key):
            list_output += '{0} '.format(key)
    LOG.info(output)
    LOG.info(list_output)


def parse_arguments():
    parser = argparse.ArgumentParser('Check what data has been cleaned')
    parser.add_argument('bucket', help='the bucket to access')
    parser.add_argument('-w', '--width', type=int, help='the frequency width', default=4)
    parser.add_argument('-i', '--iterations', type=int, help='the iterations of clean', default=10)
    parser.add_argument('-v', '--verbosity', action='count', default=0, help='increase output verbosity')
    return parser.parse_args()


def main():
    arguments = parse_arguments()
    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)
    bucket = s3.Bucket(arguments.bucket)

    # Get the data we need
    clean_entries = get_clean(bucket, arguments.width, arguments.iterations)

    analyse_data(clean_entries, arguments.width)


if __name__ == "__main__":
    main()
