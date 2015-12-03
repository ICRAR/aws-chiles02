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
Copy the CVEL output from S3 so we can run clean on it
"""
import argparse
from contextlib import closing
import logging
import os
import shutil
import sys
import tarfile

from settings_file import CHILES_BUCKET_NAME
from s3_helper import S3Helper

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)

LOG.info('PYTHONPATH = {0}'.format(sys.path))
if os.path.exists('/scratch'):
    DIRECTORY = '/scratch/kevin/split_vis'
else:
    DIRECTORY = '/tmp/kevin/split_vis'


class KeyData:
    def __init__(self, key, filename):
        self.key = key
        self.filename = filename


def copy_files(key, key_data):
    # Queue the copy of the file
    elements = key.split('/')
    directory = '{0}/{1}/{2}/'.format(DIRECTORY, elements[1], elements[2])
    tar_file = os.path.join(directory, 'data.tar.gz' if key_data.filename.endswith('data.tar.gz') else 'data.tar')
    # noinspection PyBroadException
    try:
        LOG.info('key: {0}, tar_file: {1}, directory: {2}, frequency_id: {3}'.format(
            key_data.key.key,
            tar_file,
            directory,
            elements[1]))
        if os.path.exists(directory):
            LOG.info('directory already exists: {0}'.format(directory))
        else:
            os.makedirs(directory)
            key_data.key.get_contents_to_filename(tar_file)
            with closing(tarfile.open(tar_file, "r:gz" if tar_file.endswith('tar.gz') else "r:")) as tar:
                tar.extractall(path=directory)

            os.remove(tar_file)
    except Exception:
        LOG.exception('Task died')
        shutil.rmtree(directory, ignore_errors=True)


def get_list_data():
    s3_helper = S3Helper()
    bucket = s3_helper.get_bucket(CHILES_BUCKET_NAME)
    LOG.info('Scanning bucket: {0}'.format(bucket))
    file_list = {}
    for key in bucket.list(prefix='CVEL/'):
        # Ignore the key
        if key.key.endswith('/data.tar.gz') or key.key.endswith('/data.tar'):
            (head, tail) = os.path.split(key.key)
            element = file_list.get(head)
            if element is None:
                file_list[head] = KeyData(key, tail)
            elif tail.endswith('data.tar'):
                file_list[head] = KeyData(key, tail)

    keys = file_list.keys()
    keys = sorted(keys)

    return keys, file_list


def command_pbs(args):
    # The id is the array id
    array_id = args.array_id

    keys, file_list = get_list_data()
    key = keys[array_id]
    key_data = file_list.get(key)
    copy_files(key, key_data)


def command_local_test(args):
    # The id is the index
    index = args.index

    keys, file_list = get_list_data()
    key = keys[index]
    key_data = file_list.get(key)
    copy_files(key, key_data)


def command_list(args):
    if args is not None:
        LOG.error('args should be none')
    keys, file_list = get_list_data()

    for key in keys:
        LOG.info('{0}'.format(key))

    LOG.info('Size: {0}'.format(len(keys)))


def main():
    parser = argparse.ArgumentParser('Copy the CVEL output from S3')
    subparsers = parser.add_subparsers()

    parser_pbs = subparsers.add_parser('pbs', help='pbs help')
    parser_pbs.add_argument('array_id', type=int, help='the array id from the PDS job')
    parser_pbs.set_defaults(func=command_pbs)

    parser_galaxy = subparsers.add_parser('list', help='list help')
    parser_galaxy.set_defaults(func=command_list)

    parser_galaxy = subparsers.add_parser('local_test', help='local test help')
    parser_galaxy.add_argument('index', type=int, help='the index for the command line')
    parser_galaxy.set_defaults(func=command_local_test)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
