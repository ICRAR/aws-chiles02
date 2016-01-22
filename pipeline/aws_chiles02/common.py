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
Common code to
"""

FREQUENCY_WIDTH = 4
FREQUENCY_GROUPS = []
COUNTERS = {}
INPUT_MS_SUFFIX = '_calibrated_deepfield.ms'

CONTAINER_JAVA_S3_COPY = 'sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/java-s3-copy:latest'
CONTAINER_CHILES02 = 'sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/chiles02:latest'

# for bottom_freq in range(1200, 1204, FREQUENCY_WIDTH):
for bottom_freq in range(940, 1424, FREQUENCY_WIDTH):
    FREQUENCY_GROUPS.append([bottom_freq, bottom_freq + FREQUENCY_WIDTH])


def make_groups_of_frequencies(group_size):
    groups = []
    count = 0
    batch = []
    for frequency_group in FREQUENCY_GROUPS:
        batch.append(frequency_group)

        count += 1
        if count == group_size:
            groups.append(batch)
            batch = []
            count = 0

    if len(batch) > 0:
        groups.append(batch)

    return groups


def get_oid(count_type):
    count = COUNTERS.get(count_type)
    if count is None:
        count = 1
    else:
        count += 1
    COUNTERS[count_type] = count

    return '{0}__{1:06d}'.format(count_type, count)


def get_module_name(item):
    return item.__module__ + '.' + item.__name__
