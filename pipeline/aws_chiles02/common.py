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
import Queue
import base64
import getpass
import logging
import os
import subprocess
import threading
import time
import uuid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from os.path import dirname, join, expanduser

from configobj import ConfigObj

from aws_chiles02.settings_file import INPUT_MS_SUFFIX

LOG = logging.getLogger(__name__)
COUNTERS = {}


class FrequencyPair:
    def __init__(self, bottom_frequency, top_frequency):
        self.bottom_frequency = bottom_frequency
        self.top_frequency = top_frequency
        self.name = '{0}_{1}'.format(bottom_frequency, top_frequency)

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.name == other.name


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
    [\
[940_944, 972_976, 1004_1008, 1036_1040, 1068_1072, 1100_1104, 1132_1136, 1164_1168, 1196_1200, 1228_1232, 1260_1264, 1292_1296, 1324_1328, 1356_1360, 1388_1392, 1420_1424], \
[944_948, 976_980, 1008_1012, 1040_1044, 1072_1076, 1104_1108, 1136_1140, 1168_1172, 1200_1204, 1232_1236, 1264_1268, 1296_1300, 1328_1332, 1360_1364, 1392_1396], \
[948_952, 980_984, 1012_1016, 1044_1048, 1076_1080, 1108_1112, 1140_1144, 1172_1176, 1204_1208, 1236_1240, 1268_1272, 1300_1304, 1332_1336, 1364_1368, 1396_1400], \
[952_956, 984_988, 1016_1020, 1048_1052, 1080_1084, 1112_1116, 1144_1148, 1176_1180, 1208_1212, 1240_1244, 1272_1276, 1304_1308, 1336_1340, 1368_1372, 1400_1404], \
[956_960, 988_992, 1020_1024, 1052_1056, 1084_1088, 1116_1120, 1148_1152, 1180_1184, 1212_1216, 1244_1248, 1276_1280, 1308_1312, 1340_1344, 1372_1376, 1404_1408], \
[960_964, 992_996, 1024_1028, 1056_1060, 1088_1092, 1120_1124, 1152_1156, 1184_1188, 1216_1220, 1248_1252, 1280_1284, 1312_1316, 1344_1348, 1376_1380, 1408_1412], \
[964_968, 996_1000, 1028_1032, 1060_1064, 1092_1096, 1124_1128, 1156_1160, 1188_1192, 1220_1224, 1252_1256, 1284_1288, 1316_1320, 1348_1352, 1380_1384, 1412_1416], \
[968_972, 1000_1004, 1032_1036, 1064_1068, 1096_1100, 1128_1132, 1160_1164, 1192_1196, 1224_1228, 1256_1260, 1288_1292, 1320_1324, 1352_1356, 1384_1388, 1416_1420]]
    >>> make_groups_of_frequencies(get_list_frequency_groups(8), 12)
    [\
[940_948, 1036_1044, 1132_1140, 1228_1236, 1324_1332, 1420_1428], \
[948_956, 1044_1052, 1140_1148, 1236_1244, 1332_1340], \
[956_964, 1052_1060, 1148_1156, 1244_1252, 1340_1348], \
[964_972, 1060_1068, 1156_1164, 1252_1260, 1348_1356], \
[972_980, 1068_1076, 1164_1172, 1260_1268, 1356_1364], \
[980_988, 1076_1084, 1172_1180, 1268_1276, 1364_1372], \
[988_996, 1084_1092, 1180_1188, 1276_1284, 1372_1380], \
[996_1004, 1092_1100, 1188_1196, 1284_1292, 1380_1388], \
[1004_1012, 1100_1108, 1196_1204, 1292_1300, 1388_1396], \
[1012_1020, 1108_1116, 1204_1212, 1300_1308, 1396_1404], \
[1020_1028, 1116_1124, 1212_1220, 1308_1316, 1404_1412], \
[1028_1036, 1124_1132, 1220_1228, 1316_1324, 1412_1420]]
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


def get_oid(count_type):
    count = COUNTERS.get(count_type)
    if count is None:
        count = 1
    else:
        count += 1
    COUNTERS[count_type] = count

    return '{0}__{1:06d}'.format(count_type, count)


def get_uid():
    return str(uuid.uuid4())


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


class AsynchronousFileReader(threading.Thread):
    """
    Helper class to implement asynchronous reading of a file
    in a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    """
    def __init__(self, fd, queue):
        assert isinstance(queue, Queue.Queue)
        assert callable(fd.readline)
        threading.Thread.__init__(self)
        self._fd = fd
        self._queue = queue

    def run(self):
        """
        The body of the tread: read lines and put them on the queue.
        """
        for line in iter(self._fd.readline, ''):
            self._queue.put(line)

    def eof(self):
        """
        Check whether there is no more content to expect.
        """
        return not self.is_alive() and self._queue.empty()


def run_command(command):
    LOG.info(command)
    process = subprocess.Popen(command, bufsize=1, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ.copy())

    # Launch the asynchronous readers of the processes stdout and stderr.
    stdout_queue = Queue.Queue()
    stdout_reader = AsynchronousFileReader(process.stdout, stdout_queue)
    stdout_reader.start()
    stderr_queue = Queue.Queue()
    stderr_reader = AsynchronousFileReader(process.stderr, stderr_queue)
    stderr_reader.start()

    # Check the queues if we received some output (until there is nothing more to get).
    while not stdout_reader.eof() or not stderr_reader.eof():
        # Show what we received from standard output.
        while not stdout_queue.empty():
            line = stdout_queue.get()
            print line.rstrip()

        # Show what we received from standard error.
        while not stderr_queue.empty():
            line = stderr_queue.get()
            print line.rstrip()

        # Sleep a bit before asking the readers again.
        time.sleep(2)

    # Let's be tidy and join the threads we've started.
    stdout_reader.join()
    stderr_reader.join()

    # Close subprocess' file descriptors.
    process.stdout.close()
    process.stderr.close()

    return_code = process.poll()
    return return_code


def get_argument(config, key, prompt, help_text=None, data_type=None, default=None):
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

    if data_type is not None:
        if data_type == int:
            config[key] = int(data)
        elif data_type == float:
            config[key] = float(data)
        elif data_type == bool:
            config[key] = bool(data)
    else:
        config[key] = data


def get_user_data(cloud_init_data):
    user_data = MIMEMultipart()
    for cloud_init in cloud_init_data:
        user_data.attach(MIMEText(cloud_init))

    encode = user_data.as_string().encode("ascii")
    encoded_data = base64.b64encode(encode).decode('ascii')

    return encoded_data


def get_file_contents(file_name):
    here = dirname(__file__)
    bash = join(here, '../user_data', file_name)
    with open(bash, 'r') as my_file:
        data = my_file.read()
    return data


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
