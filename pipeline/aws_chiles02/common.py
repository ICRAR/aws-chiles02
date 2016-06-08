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
import threading
import time
import uuid
from cStringIO import StringIO
from os.path import join, expanduser

from configobj import ConfigObj

from aws_chiles02.settings_file import INPUT_MS_SUFFIX, INPUT_MS_SUFFIX_TAR

LOG = logging.getLogger(__name__)


class FrequencyPair:
    def __init__(self, bottom_frequency, top_frequency):
        self.bottom_frequency = bottom_frequency
        self.top_frequency = top_frequency
        self._name = 'FrequencyPair({0}, {1})'.format(bottom_frequency, top_frequency)
        self._underscore_name = '{0}_{1}'.format(bottom_frequency, top_frequency)

    def __str__(self):
        return self._name

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash((self.bottom_frequency, self.top_frequency))

    def __eq__(self, other):
        return self._name == other.name

    @property
    def name(self):
        return self._name

    @property
    def underscore_name(self):
        return self._underscore_name


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
    for bottom_frequency in range(944, 1420, frequency_width):
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
            time.strftime('%Y%m%d%H%M%S')
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


class OutputStream(threading.Thread):
    def __init__(self):
        super(OutputStream, self).__init__()
        self.done = False
        self.buffer = StringIO()
        self.read, self.write = os.pipe()
        self.reader = os.fdopen(self.read)
        self.start()

    def fileno(self):
        return self.write

    def run(self):
        while not self.done:
            self.buffer.write(self.reader.readline())

        self.reader.close()

    def close(self):
        self.done = True
        os.close(self.write)

    def __enter__(self):
        # Theoretically could be used to set up things not in __init__
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def run_command(command):
    LOG.info(command)
    with OutputStream() as stream:
        process = subprocess.Popen(command, bufsize=1, shell=True, stdout=stream, stderr=subprocess.STDOUT, env=os.environ.copy())
        while process.poll() is None:
            time.sleep(1)

    output = stream.buffer.getvalue()
    LOG.info('{0}, output follows.\n{1}'.format(command, output))

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
            config[key] = data in ['True', 'true']
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


class ProgressPercentage:
    def __init__(self, filename, expected_size):
        self._filename = filename
        self._size = float(expected_size)
        self._size_mb = bytes2human(expected_size, '{0:.2f}{1}')
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._percentage = -1

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            if self._size >= 1.0:
                percentage = int((self._seen_so_far / self._size) * 100.0)
                if percentage > self._percentage:
                    LOG.info(
                        '{0}  {1} / {2} ({3}%)'.format(
                            self._filename,
                            bytes2human(self._seen_so_far, '{0:.2f}{1}'),
                            self._size_mb,
                            percentage))
                    self._percentage = percentage
            else:
                LOG.warning('Filename: {0}, size: 0'.format(self._filename))


SYMBOLS = {
    'customary': ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_ext': ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa', 'zetta', 'iotta'),
    'iec': ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'iec_ext': ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi', 'zebi', 'yobi'),
}


def bytes2human(n, format_string='{0:.1f}{1}', symbols='customary'):
    """
    Convert n bytes into a human readable string based on format.
    symbols can be either "customary", "customary_ext", "iec" or "iec_ext",
    see: http://goo.gl/kTQMs

      >>> bytes2human(0)
      '0.0B'
      >>> bytes2human(0.9)
      '0.0B'
      >>> bytes2human(1)
      '1.0B'
      >>> bytes2human(1.9)
      '1.0B'
      >>> bytes2human(1024)
      '1.0K'
      >>> bytes2human(1048576)
      '1.0M'
      >>> bytes2human(1099511627776127398123789121)
      '909.5Y'

      >>> bytes2human(9856, symbols="customary")
      '9.6K'
      >>> bytes2human(9856, symbols="customary_ext")
      '9.6kilo'
      >>> bytes2human(9856, symbols="iec")
      '9.6Ki'
      >>> bytes2human(9856, symbols="iec_ext")
      '9.6kibi'

      >>> bytes2human(10000, "{0:.1f} {1}/sec")
      '9.8 K/sec'

      >>> # precision can be adjusted by playing with %f operator
      >>> bytes2human(10000, format_string="{0:.5f} {1}")
      '9.76562 K'
    """
    n = int(n)
    if n < 0:
        raise ValueError("n < 0")

    symbols = SYMBOLS[symbols]
    prefix = {}
    for i, s in enumerate(symbols[1:]):
        prefix[s] = 1 << (i + 1) * 10
    for symbol in reversed(symbols[1:]):
        if n >= prefix[symbol]:
            value = float(n) / prefix[symbol]
            return format_string.format(value, symbol)
    return format_string.format(n, symbols[0])


def human2bytes(input_string):
    """
    Attempts to guess the string format based on default symbols
    set and return the corresponding bytes as an integer.
    When unable to recognize the format ValueError is raised.

      >>> human2bytes('0 B')
      0
      >>> human2bytes('1 K')
      1024
      >>> human2bytes('1M')
      1048576
      >>> human2bytes('1 Gi')
      1073741824
      >>> human2bytes('1 tera')
      1099511627776
      >>> human2bytes('0.5kilo')
      512
      >>> human2bytes('0.1  byte')
      0
      >>> human2bytes('1 k')  # k is an alias for K
      1024
      >>> human2bytes('12 foo')
      Traceback (most recent call last):
          ...
      ValueError: can't interpret '12 foo'
    """
    init = input_string
    num = ""
    while input_string and input_string[0:1].isdigit() or input_string[0:1] == '.':
        num += input_string[0]
        input_string = input_string[1:]
    num = float(num)
    letter = input_string.strip()
    for name, sset in SYMBOLS.items():
        if letter in sset:
            break
    else:
        if letter == 'k':
            # treat 'k' as an alias for 'K' as per: http://goo.gl/kTQMs
            sset = SYMBOLS['customary']
            letter = letter.upper()
        else:
            raise ValueError("can't interpret '{0}'".format(init))

    prefix = {
        sset[0]: 1
    }
    for i, input_string in enumerate(sset[1:]):
        prefix[input_string] = 1 << (i + 1) * 10
    return int(num * prefix[letter])


def set_logging_level(verbosity):
    if verbosity == 0:
        logging.basicConfig(level=logging.INFO)
        set_boto_logging_level(level=logging.WARN)
    elif verbosity == 1:
        logging.basicConfig(level=logging.INFO)
        set_boto_logging_level(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.DEBUG)
        set_boto_logging_level(level=logging.DEBUG)


def set_boto_logging_level(level):
    logging.getLogger('boto3').setLevel(level)
    logging.getLogger('botocore').setLevel(level)
    logging.getLogger('nose').setLevel(level)
