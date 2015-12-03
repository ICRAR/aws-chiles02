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
import collections
import logging

from ec2_helper import EC2Helper
from s3_helper import S3Helper
from settings_file import CHILES_BUCKET_NAME, FREQUENCY_GROUPS


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)


def get_snapshots():
    ec2_helper = EC2Helper()
    connection = ec2_helper.ec2_connection
    snapshots = []
    for snapshot in connection.get_all_snapshots(owner='self'):
        name = snapshot.tags.get('Name')
        if name is None:
            LOG.info('Looking at {0} - None'.format(snapshot.id))
        elif snapshot.status == 'completed':
            LOG.info('Looking at {0} - {1}'.format(snapshot.id, snapshot.tags['Name']))
            if snapshot.tags['Name'].endswith('_FINAL_PRODUCTS'):
                snapshots.append(str(name[:-15]))
        else:
            LOG.info('Looking at {0} - {1} which is {2}'.format(snapshot.id, snapshot.tags['Name'], snapshot.status))

    return snapshots


def get_cvel():
    s3_helper = S3Helper()
    bucket = s3_helper.get_bucket(CHILES_BUCKET_NAME)
    cvel_data = []
    for key in bucket.list(prefix='CVEL/'):
        LOG.info('Checking {0}'.format(key.key))
        if key.key.endswith('data.tar.gz') or key.key.endswith('data.tar'):
            elements = key.key.split('/')
            cvel_data.append([str(elements[2]), str(elements[1])])

    return cvel_data


def swap_underscores(text):
    return text.replace('-', '_')


def analyse_data(snapshots, cvel_entries):
    # Build the expected list
    expected_combinations = {}
    for key in snapshots:
        bottom_frequency = int(key.split('_')[1])
        list_data = []
        for frequency in FREQUENCY_GROUPS:
            if bottom_frequency < frequency[0]:
                list_data.append('vis_{0}~{1}'.format(frequency[0], frequency[1]))
        expected_combinations[key] = list_data

    for element in cvel_entries:
        key = swap_underscores(element[0])
        frequencies = expected_combinations[key]
        if element[1] in frequencies:
            frequencies.remove(element[1])

    number_entries = len(FREQUENCY_GROUPS)
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
    # Get the data we need
    snapshots = get_snapshots()
    cvel_entries = get_cvel()

    analyse_data(snapshots, cvel_entries)


if __name__ == "__main__":
    main()
