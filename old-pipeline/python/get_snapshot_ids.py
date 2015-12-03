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
Get the snapshot ids and dates
"""
import collections
import logging

from ec2_helper import EC2Helper


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)


def main():
    ec2_helper = EC2Helper()
    connection = ec2_helper.ec2_connection
    settings = {}
    for snapshot in connection.get_all_snapshots(owner='self'):
        name = snapshot.tags.get('Name')
        if name is None:
            LOG.info('Looking at {0} - None'.format(snapshot.id))
        elif snapshot.status == 'completed':
            LOG.info('Looking at {0} - {1}'.format(snapshot.id, snapshot.tags['Name']))
            if snapshot.tags['Name'].endswith('_FINAL_PRODUCTS'):
                settings[name[:-15]] = snapshot.id
        else:
            LOG.info('Looking at {0} - {1} which is {2}'.format(snapshot.id, snapshot.tags['Name'], snapshot.status))
    ordered_dictionary = collections.OrderedDict(sorted(settings.items()))

    output = '\n'

    for key, value in ordered_dictionary.iteritems():
        output += '''{0} = "{1}"
'''.format(key, value)

    LOG.info(output)

if __name__ == "__main__":
    main()
