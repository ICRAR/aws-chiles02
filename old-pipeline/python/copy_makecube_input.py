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
import multiprocessing
import os
import shutil
import sys
import tarfile
from contextlib import closing
from os.path import basename, exists, join

from common import LOGGER, Consumer
from echo import echo
from settings_file import CHILES_BUCKET_NAME
from s3_helper import S3Helper


LOGGER.info('PYTHONPATH = {0}'.format(sys.path))
DIRECTORY = '/mnt/input/Chiles/'


class Task(object):
    """
    The actual task
    """
    def __init__(self, key, tar_file, directory):
        self._key = key
        self._tar_file = tar_file
        self._directory = directory

    def __call__(self):
        """
        Actually run the job
        """
        if self._tar_file.endswith('.tar.gz'):
            image_name = basename(self._tar_file).replace('.tar.gz', '')
        else:
            image_name = basename(self._tar_file).replace('.tar', '')
        directory = join(self._directory, image_name)
        # noinspection PyBroadException
        try:
            LOGGER.info('key: {0}, tar_file: {1}, directory: {2}'.format(self._key.key, self._tar_file, directory))
            if not os.path.exists(directory):
                os.makedirs(directory)
            self._key.get_contents_to_filename(self._tar_file)
            with closing(tarfile.open(self._tar_file, "r:gz" if self._tar_file.endswith('.tar.gz') else "r:")) as tar:
                tar.extractall(path=directory)

            os.remove(self._tar_file)
        except Exception:
            LOGGER.exception('Task died')
            shutil.rmtree(directory, ignore_errors=True)


@echo
def in_frequency_range(key, bottom_frequency, frequency_range):
    """
    >>> in_frequency_range('vis_1200~1204.image.tar', 1200, 100)
    True

    >>> in_frequency_range('vis_1200~1204.image.tar', 1100, 100)
    False

    >>> in_frequency_range('vis_1204~1208.image.tar', 1200, 100)
    True

    >>> in_frequency_range('vis_1296~1300.image.tar', 1200, 100)
    True

    >>> in_frequency_range('vis_1296~1300.image.tar', 1300, 100)
    False

    :param key:
    :param bottom_frequency:
    :param frequency_range:
    :return:
    """
    elements = key.split('.')
    elements = elements[0].split('_')
    elements = elements[1].split('~')
    return int(elements[0]) >= bottom_frequency and int(elements[1]) <= bottom_frequency + frequency_range


def copy_files(processes, bottom_frequency, frequency_range):
    # Create the directory
    if not exists(DIRECTORY):
        os.makedirs(DIRECTORY)

    # Scan the bucket
    s3_helper = S3Helper()
    bucket = s3_helper.get_bucket(CHILES_BUCKET_NAME)
    LOGGER.info('Scanning bucket: {0}/CLEAN'.format(bucket))

    # Create the queue
    queue = multiprocessing.JoinableQueue()

    # Start the consumers
    for x in range(processes):
        consumer = Consumer(queue)
        consumer.start()

    for key in bucket.list(prefix='CLEAN/'):
        LOGGER.info('Checking {0}'.format(key.key))
        # Ignore the key
        if key.key.endswith('.image.tar.gz') or key.key.endswith('.image.tar'):
            # Do we need this file?
            basename_key = basename(key.key)
            if in_frequency_range(basename_key, bottom_frequency, frequency_range):
                # Queue the copy of the file
                temp_file = os.path.join(DIRECTORY, basename_key)
                queue.put(Task(key, temp_file, DIRECTORY))

    # Add a poison pill to shut things down
    for x in range(processes):
        queue.put(None)

    # Wait for the queue to terminate
    queue.join()


def main():
    parser = argparse.ArgumentParser('Copy the CLEAN output from S3')
    parser.add_argument('-p', '--processes', type=int, default=1, help='the number of processes to run')
    parser.add_argument('bottom_frequency', type=int, help='The bottom frequency')
    parser.add_argument('frequency_range', type=int, help='the range of frequencies')

    args = vars(parser.parse_args())

    copy_files(args['processes'], args['bottom_frequency'], args['frequency_range'])

if __name__ == "__main__":
    main()
