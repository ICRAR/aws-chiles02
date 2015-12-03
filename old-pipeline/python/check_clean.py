#
#    (c) UWA, The University of Western Australia
#    M468/35 Stirling Hwy
#    Perth WA 6009
#    Australia
#
#    Copyright by UWA, 2012-2015
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

from s3_helper import S3Helper
from settings_file import CHILES_BUCKET_NAME, FREQUENCY_GROUPS


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)


def get_clean():
    s3_helper = S3Helper()
    bucket = s3_helper.get_bucket(CHILES_BUCKET_NAME)
    clean_data = set()
    for key in bucket.list(prefix='CLEAN/'):
        if key.key.endswith('image.tar.gz') or key.key.endswith('image.tar'):
            LOG.info('Checking {0}'.format(key.key))
            elements = key.key.split('/')
            clean_data.add(elements[1])

    return clean_data


def analyse_data(clean_entries):
    # Build the expected list
    expected_combinations = ['vis_{0}~{1}'.format(frequency[0], frequency[1]) for frequency in FREQUENCY_GROUPS ]

    output = '\n'
    list_output = '\n'
    for key in sorted(expected_combinations):
        output += '{0} = {1}\n'.format(key, 'Done' if clean_entries.__contains__(key) else 'Not done')
        if not clean_entries.__contains__(key):
            list_output += '{0} '.format(key)
    LOG.info(output)
    LOG.info(list_output)


def main():
    # Get the data we need
    clean_entries = get_clean()

    analyse_data(clean_entries)


if __name__ == "__main__":
    main()
