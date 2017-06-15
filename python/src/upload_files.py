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

"""
import argparse
import threading

from os.path import exists, isdir, getsize, join

import boto3
import logging

from os import listdir
from s3transfer import S3Transfer

LOG = logging.getLogger(__name__)
SYMBOLS = {
    'customary': ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_ext': ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa', 'zetta', 'iotta'),
    'iec': ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'iec_ext': ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi', 'zebi', 'yobi'),
}


def bytes2human(n, format_string='{0:.1f}{1}', symbols='customary'):
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


def upload_data(bucket_name, folder_name, source_folder):
    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)

    for filename in listdir(source_folder):
        source_file = join(source_folder, filename)
        key = '{0}/{1}'.format(folder_name, filename)
        s3_client = s3.meta.client
        transfer = S3Transfer(s3_client)
        transfer.upload_file(
            source_file,
            bucket_name,
            key,
            callback=ProgressPercentage(
                key,
                float(getsize(source_file))
            ),
            extra_args={
                'StorageClass': 'REDUCED_REDUNDANCY',
            }
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser('Get the arguments')
    parser.add_argument('bucket_name', help='the bucket name')
    parser.add_argument('folder_name', help='the folder in the bucket with the data')
    parser.add_argument('source_folder', help='the folder with the source data')

    args = parser.parse_args()

    if exists(args.source_folder) and isdir(args.source_folder):
        upload_data(args.bucket_name, args.folder_name, args.source_folder)
