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
import getpass
import logging
import os
import subprocess
import time
import uuid
from os.path import join, expanduser

from configobj import ConfigObj

from aws_chiles02.settings_file import INPUT_MS_SUFFIX, INPUT_MS_SUFFIX_TAR

LOG = logging.getLogger(__name__)


class FrequencyPair:
    def __init__(self, bottom_frequency, top_frequency):
        self.bottom_frequency = bottom_frequency
        self.top_frequency = top_frequency
        self.name = 'FrequencyPair({0}, {1})'.format(bottom_frequency, top_frequency)

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash((self.bottom_frequency, self.top_frequency))

    def __eq__(self, other):
        return self.name == other.name


class MeasurementSetData:
    def __init__(self, full_tar_name, size):
        self.full_tar_name = full_tar_name
        self.size = size
        # Get rid of the '_calibrated_deepfield.ms.tar'
        self.short_name = full_tar_name[:-len(INPUT_MS_SUFFIX_TAR)]

    def __str__(self):
        return 'MeasurementSetData(\'{0}\', {1})'.format(self.short_name, self.size)

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash((self.full_tar_name, self.size))

    def __eq__(self, other):
        return (self.full_tar_name, self.size) == (other.full_tar_name, other.size)


def get_list_frequency_groups(frequency_width):
    list_frequencies = []
    for bottom_frequency in range(940, 1424, frequency_width):
        list_frequencies.append(FrequencyPair(bottom_frequency, bottom_frequency + frequency_width))
    return list_frequencies


def make_groups_of_frequencies(frequencies_to_batch_up, number_of_groups):
    """

    :param frequencies_to_batch_up:
    :param number_of_groups:
    :return:
    >>> make_groups_of_frequencies(get_list_frequency_groups(4), 8)
    [[FrequencyPair(940, 944), FrequencyPair(972, 976), FrequencyPair(1004, 1008), FrequencyPair(1036, 1040), FrequencyPair(1068, 1072), FrequencyPair(1100, 1104), FrequencyPair(1132, 1136), FrequencyPair(1164, 1168), FrequencyPair(1196, 1200), FrequencyPair(1228, 1232), FrequencyPair(1260, 1264), FrequencyPair(1292, 1296), FrequencyPair(1324, 1328), FrequencyPair(1356, 1360), FrequencyPair(1388, 1392), FrequencyPair(1420, 1424)], [FrequencyPair(944, 948), FrequencyPair(976, 980), FrequencyPair(1008, 1012), FrequencyPair(1040, 1044), FrequencyPair(1072, 1076), FrequencyPair(1104, 1108), FrequencyPair(1136, 1140), FrequencyPair(1168, 1172), FrequencyPair(1200, 1204), FrequencyPair(1232, 1236), FrequencyPair(1264, 1268), FrequencyPair(1296, 1300), FrequencyPair(1328, 1332), FrequencyPair(1360, 1364), FrequencyPair(1392, 1396)], [FrequencyPair(948, 952), FrequencyPair(980, 984), FrequencyPair(1012, 1016), FrequencyPair(1044, 1048), FrequencyPair(1076, 1080), FrequencyPair(1108, 1112), FrequencyPair(1140, 1144), FrequencyPair(1172, 1176), FrequencyPair(1204, 1208), FrequencyPair(1236, 1240), FrequencyPair(1268, 1272), FrequencyPair(1300, 1304), FrequencyPair(1332, 1336), FrequencyPair(1364, 1368), FrequencyPair(1396, 1400)], [FrequencyPair(952, 956), FrequencyPair(984, 988), FrequencyPair(1016, 1020), FrequencyPair(1048, 1052), FrequencyPair(1080, 1084), FrequencyPair(1112, 1116), FrequencyPair(1144, 1148), FrequencyPair(1176, 1180), FrequencyPair(1208, 1212), FrequencyPair(1240, 1244), FrequencyPair(1272, 1276), FrequencyPair(1304, 1308), FrequencyPair(1336, 1340), FrequencyPair(1368, 1372), FrequencyPair(1400, 1404)], [FrequencyPair(956, 960), FrequencyPair(988, 992), FrequencyPair(1020, 1024), FrequencyPair(1052, 1056), FrequencyPair(1084, 1088), FrequencyPair(1116, 1120), FrequencyPair(1148, 1152), FrequencyPair(1180, 1184), FrequencyPair(1212, 1216), FrequencyPair(1244, 1248), FrequencyPair(1276, 1280), FrequencyPair(1308, 1312), FrequencyPair(1340, 1344), FrequencyPair(1372, 1376), FrequencyPair(1404, 1408)], [FrequencyPair(960, 964), FrequencyPair(992, 996), FrequencyPair(1024, 1028), FrequencyPair(1056, 1060), FrequencyPair(1088, 1092), FrequencyPair(1120, 1124), FrequencyPair(1152, 1156), FrequencyPair(1184, 1188), FrequencyPair(1216, 1220), FrequencyPair(1248, 1252), FrequencyPair(1280, 1284), FrequencyPair(1312, 1316), FrequencyPair(1344, 1348), FrequencyPair(1376, 1380), FrequencyPair(1408, 1412)], [FrequencyPair(964, 968), FrequencyPair(996, 1000), FrequencyPair(1028, 1032), FrequencyPair(1060, 1064), FrequencyPair(1092, 1096), FrequencyPair(1124, 1128), FrequencyPair(1156, 1160), FrequencyPair(1188, 1192), FrequencyPair(1220, 1224), FrequencyPair(1252, 1256), FrequencyPair(1284, 1288), FrequencyPair(1316, 1320), FrequencyPair(1348, 1352), FrequencyPair(1380, 1384), FrequencyPair(1412, 1416)], [FrequencyPair(968, 972), FrequencyPair(1000, 1004), FrequencyPair(1032, 1036), FrequencyPair(1064, 1068), FrequencyPair(1096, 1100), FrequencyPair(1128, 1132), FrequencyPair(1160, 1164), FrequencyPair(1192, 1196), FrequencyPair(1224, 1228), FrequencyPair(1256, 1260), FrequencyPair(1288, 1292), FrequencyPair(1320, 1324), FrequencyPair(1352, 1356), FrequencyPair(1384, 1388), FrequencyPair(1416, 1420)]]
    >>> make_groups_of_frequencies(get_list_frequency_groups(8), 12)
    [[FrequencyPair(940, 948), FrequencyPair(1036, 1044), FrequencyPair(1132, 1140), FrequencyPair(1228, 1236), FrequencyPair(1324, 1332), FrequencyPair(1420, 1428)], [FrequencyPair(948, 956), FrequencyPair(1044, 1052), FrequencyPair(1140, 1148), FrequencyPair(1236, 1244), FrequencyPair(1332, 1340)], [FrequencyPair(956, 964), FrequencyPair(1052, 1060), FrequencyPair(1148, 1156), FrequencyPair(1244, 1252), FrequencyPair(1340, 1348)], [FrequencyPair(964, 972), FrequencyPair(1060, 1068), FrequencyPair(1156, 1164), FrequencyPair(1252, 1260), FrequencyPair(1348, 1356)], [FrequencyPair(972, 980), FrequencyPair(1068, 1076), FrequencyPair(1164, 1172), FrequencyPair(1260, 1268), FrequencyPair(1356, 1364)], [FrequencyPair(980, 988), FrequencyPair(1076, 1084), FrequencyPair(1172, 1180), FrequencyPair(1268, 1276), FrequencyPair(1364, 1372)], [FrequencyPair(988, 996), FrequencyPair(1084, 1092), FrequencyPair(1180, 1188), FrequencyPair(1276, 1284), FrequencyPair(1372, 1380)], [FrequencyPair(996, 1004), FrequencyPair(1092, 1100), FrequencyPair(1188, 1196), FrequencyPair(1284, 1292), FrequencyPair(1380, 1388)], [FrequencyPair(1004, 1012), FrequencyPair(1100, 1108), FrequencyPair(1196, 1204), FrequencyPair(1292, 1300), FrequencyPair(1388, 1396)], [FrequencyPair(1012, 1020), FrequencyPair(1108, 1116), FrequencyPair(1204, 1212), FrequencyPair(1300, 1308), FrequencyPair(1396, 1404)], [FrequencyPair(1020, 1028), FrequencyPair(1116, 1124), FrequencyPair(1212, 1220), FrequencyPair(1308, 1316), FrequencyPair(1404, 1412)], [FrequencyPair(1028, 1036), FrequencyPair(1124, 1132), FrequencyPair(1220, 1228), FrequencyPair(1316, 1324), FrequencyPair(1412, 1420)]]
    """
    groups = []
    for group in range(0, number_of_groups):
        groups.append([])
    count = 0
    for frequency_group in frequencies_to_batch_up:
        mod = count % number_of_groups
        groups[mod].append(frequency_group)
        count += 1

    return groups


def get_module_name(item):
    return item.__module__ + '.' + item.__name__


def get_session_id():
    return '{0}-{1}'.format(
            getpass.getuser(),
            int(time.time())
    )


def split_s3_url(s3_url):
    """
    Split the s3 url into bucket and key
    :param s3_url:
    :return:

    >>> split_s3_url('s3://bucket_name/key/morekey/ms.tar')
    ('bucket_name', 'key/morekey/ms.tar')
    """
    body = s3_url[5:]
    index = body.find('/')
    return body[:index], body[index+1:]


def get_observation(s3_path):
    if s3_path.endswith('.tar'):
        s3_path = s3_path[:-4]
    elements = s3_path[:-len(INPUT_MS_SUFFIX)]
    return elements


def run_command(command):
    LOG.info(command)
    process = subprocess.Popen(command, bufsize=1, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ.copy())

    stdout, stderr = process.communicate()
    LOG.debug('{0}, output follows.\n==STDOUT==\n{1}==STDERR==\n{2}'.format(command, stdout, stderr))

    return process.returncode


def get_argument(config, key, prompt, help_text=None, data_type=None, default=None, allowed=None):
    if key in config:
        default = config[key]

    if default is not None:
        prompt = '{0} [{1}]:'.format(prompt, default)
    else:
        prompt = '{0}:'.format(prompt)

    data = None

    while data is None:
        data = raw_input(prompt)
        if data == '?':
            if help_text is not None:
                print '\n' + help_text + '\n'
            else:
                print '\nNo help available\n'

            data = None
        elif data == '':
            data = default

        if allowed is not None:
            if data not in allowed:
                data = None

    if data_type is not None:
        if data_type == int:
            config[key] = int(data)
        elif data_type == float:
            config[key] = float(data)
        elif data_type == bool:
            config[key] = bool(data)
    else:
        config[key] = data


def get_aws_credentials(profile_name):
    data = None
    dot_boto = join(expanduser('~'), '.aws', 'credentials')
    if os.path.exists(dot_boto):
        config = ConfigObj(dot_boto)
        if profile_name in config:
            data = [
                config[profile_name]['aws_access_key_id'],
                config[profile_name]['aws_secret_access_key'],
            ]
    return data


def get_uuid():
    return str(uuid.uuid4())
