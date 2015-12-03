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
import multiprocessing
import os
from os.path import join
import shutil
import sys
import tarfile

from common import Consumer, LOGGER
from settings_file import CHILES_BUCKET_NAME
from s3_helper import S3Helper


LOGGER.info('PYTHONPATH = {0}'.format(sys.path))


class Task(object):
    """
    The actual task
    """
    def __init__(self, key, tar_file, directory, frequency_id):
        self._key = key
        self._tar_file = tar_file
        self._directory = directory
        self._frequency_id = frequency_id

    def __call__(self):
        """
        Actually run the job
        """
        corrected_path = join(self._directory, self._frequency_id)
        # noinspection PyBroadException
        try:
            LOGGER.info('key: {0}, tar_file: {1}, directory: {2}, frequency_id: {3}'.format(
                self._key.key,
                self._tar_file,
                self._directory,
                self._frequency_id))
            if not os.path.exists(corrected_path):
                os.makedirs(corrected_path)
            self._key.get_contents_to_filename(self._tar_file)
            with closing(tarfile.open(self._tar_file, "r:gz" if self._tar_file.endswith('tar.gz') else "r:")) as tar:
                tar.extractall(path=corrected_path)

            os.remove(self._tar_file)
        except Exception:
            LOGGER.exception('Task died')
            shutil.rmtree(corrected_path, ignore_errors=True)


def copy_files(frequency_id, processes):
    s3_helper = S3Helper()
    bucket = s3_helper.get_bucket(CHILES_BUCKET_NAME)
    LOGGER.info('Scanning bucket: {0}, frequency_id: {1}'.format(bucket, frequency_id))

    # Create the queue
    queue = multiprocessing.JoinableQueue()

    # Start the consumers
    for x in range(processes):
        consumer = Consumer(queue)
        consumer.start()

    for key in bucket.list(prefix='CVEL/{0}'.format(frequency_id)):
        LOGGER.info('Checking {0}'.format(key.key))
        # Ignore the key
        if key.key.endswith('/data.tar.gz') or key.key.endswith('/data.tar'):
            elements = key.key.split('/')
            directory = '/mnt/output/Chiles/split_vis/{0}/'.format(elements[2])

            # Queue the copy of the file
            temp_file = os.path.join(directory, 'data.tar.gz' if key.key.endswith('/data.tar.gz') else 'data.tar')
            queue.put(Task(key, temp_file, directory, frequency_id))

    # Add a poison pill to shut things down
    for x in range(processes):
        queue.put(None)

    # Wait for the queue to terminate
    queue.join()


def main():
    parser = argparse.ArgumentParser('Copy the CVEL output from S3')
    parser.add_argument('freq_id', help='the frequency id')
    parser.add_argument('-p', '--processes', type=int, default=1, help='the number of processes to run')

    args = vars(parser.parse_args())
    frequency_id = args['freq_id']
    processes = args['processes']

    copy_files(frequency_id, processes)

if __name__ == "__main__":
    main()
