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
import argparse
import logging

import boto3

from .common import get_list_frequency_groups, set_logging_level

LOG = logging.getLogger(__name__)


def get_clean(bucket, width=None, iterations=None, arcsec=None, prefix=None):
    if prefix is None:
        prefix = 'clean_{0}_{1}_{2}'.format(width, iterations, arcsec)
    clean_data = []
    for key in bucket.objects.filter(Prefix=prefix):
        elements = key.key.split('/')
        if len(elements) >= 2:
            if len(elements[1]) > 0:
                clean_data.append(elements[1])

    return clean_data


def analyse_data(clean_entries, width):
    # Build the expected list
    expected_combinations = [
        'cleaned_{0}_{1}.tar'.format(frequency.bottom_frequency, frequency.top_frequency) for frequency in get_list_frequency_groups(width)]

    done = []
    not_done = []
    for key in sorted(expected_combinations):
        for extensions in ['', '.centre', '.qa']:
            new_key = key + extensions
            if clean_entries.__contains__(new_key):
                done.append(new_key)
            else:
                not_done.append(new_key)

    LOG.info('** NOT DONE {0} **'.format(len(not_done)))
    for key in not_done:
        LOG.info(key)

    LOG.info('** DONE {0} **'.format(len(done)))
    for key in done:
        LOG.info(key)


def parse_arguments():
    parser = argparse.ArgumentParser('Check what data has been cleaned')
    parser.add_argument('bucket', help='the bucket to access')
    parser.add_argument('-a', '--arcsec', help='the arc seconds of a pixel')
    parser.add_argument('-p', '--prefix', help='the clean prefix for custom named clean runs')
    parser.add_argument('-w', '--width', type=int, help='the frequency width', default=4)
    parser.add_argument('-i', '--iterations', type=int, help='the iterations of clean')
    parser.add_argument('-v', '--verbosity', action='count', default=0, help='increase output verbosity')
    return parser.parse_args()


def main():
    arguments = parse_arguments()
    set_logging_level(arguments.verbosity)
    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)
    bucket = s3.Bucket(arguments.bucket)

    # Get the data we need
    clean_entries = get_clean(bucket, width=arguments.width, iterations=arguments.iterations, arcsec=arguments.arcsec, prefix=arguments.prefix)

    # Is it what we expect
    analyse_data(clean_entries, arguments.width)


if __name__ == "__main__":
    main()
