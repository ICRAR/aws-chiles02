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
Copy the CLEAN output from S3
"""
import argparse
import logging
import os
import shutil
import tarfile
from contextlib import closing
from os.path import basename, exists, join
import sys

from pleiades_common import get_expect_combinations
from settings_file import CHILES_BUCKET_NAME
from s3_helper import S3Helper


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)

LOG.info('PYTHONPATH = {0}'.format(sys.path))
DIRECTORY = '/mnt/hidata/kevin/images'


def copy_files(directory_name):
    # Create the directory
    if not exists(DIRECTORY):
        os.makedirs(DIRECTORY)

    # Scan the bucket
    s3_helper = S3Helper()
    bucket = s3_helper.get_bucket(CHILES_BUCKET_NAME)
    LOG.info('Scanning bucket: {0}/CLEAN/{1}'.format(bucket, directory_name))

    for key in bucket.list(prefix='CLEAN/{0}'.format(directory_name)):
        LOG.info('Checking {0}'.format(key.key))
        # Ignore the key
        if key.key.endswith('.image.tar.gz') or key.key.endswith('.image.tar'):
            # Do we need this file?
            basename_key = basename(key.key)
            tar_file = os.path.join(DIRECTORY, basename_key)
            if tar_file.endswith('.tar.gz'):
                image_name = basename(tar_file).replace('.tar.gz', '')
            else:
                image_name = basename(tar_file).replace('.tar', '')
            directory = join(DIRECTORY, image_name)
            # noinspection PyBroadException
            try:
                LOG.info('key: {0}, tar_file: {1}, directory: {2}'.format(key.key, tar_file, directory))
                if os.path.exists(directory):
                    LOG.info('directory already exists: {0}'.format(directory))
                else:
                    os.makedirs(directory)
                    key.get_contents_to_filename(tar_file)
                    with closing(tarfile.open(tar_file, "r:gz" if tar_file.endswith('.tar.gz') else "r:")) as tar:
                        tar.extractall(path=directory)

                    os.remove(tar_file)
            except Exception:
                LOG.exception('Task died')
                shutil.rmtree(directory, ignore_errors=True)


def command_pbs(args):
    # The id is the array id
    array_id = args.array_id

    expected_combinations = get_expect_combinations()
    copy_files(expected_combinations[array_id])


def command_list(args):
    LOG.info('{0}'.format(args))
    expected_combinations = get_expect_combinations()
    count = 0
    for combination in expected_combinations:
        LOG.info('{0} - {1}'.format(count, combination))
        count += 1


def main():
    parser = argparse.ArgumentParser('Copy the CLEAN output from S3')
    subparsers = parser.add_subparsers()
    parser_pbs = subparsers.add_parser('pbs', help='pbs help')
    parser_pbs.add_argument('array_id', type=int, help='the array id from the PDS job')
    parser_pbs.set_defaults(func=command_pbs)

    parser_galaxy = subparsers.add_parser('list', help='list help')
    parser_galaxy.set_defaults(func=command_list)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
