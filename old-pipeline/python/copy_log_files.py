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
Copy the log files to S3
"""
import argparse
import fnmatch
import multiprocessing
import os
from os.path import join
import sys
import datetime

from common import Consumer, LOGGER
from settings_file import CHILES_BUCKET_NAME, CHILES_LOGS, BENCHMARKING_LOGS
from s3_helper import S3Helper


LOGGER.info('PYTHONPATH = {0}'.format(sys.path))


class CopyTask(object):
    def __init__(self, filename, bucket_location):
        self._filename = filename
        self._bucket_location = bucket_location

    def __call__(self):
        # noinspection PyBroadException
        try:
            LOGGER.info('Copying {0} to s3:{1}'.format(self._filename, self._bucket_location))
            s3_helper = S3Helper()
            s3_helper.add_file_to_bucket(
                CHILES_BUCKET_NAME,
                self._bucket_location,
                self._filename)
        except Exception:
            LOGGER.exception('CopyTask died')


def copy_files(s3_tag, processes):
    # Create the queue
    queue = multiprocessing.JoinableQueue()
    # Start the consumers
    for x in range(processes):
        consumer = Consumer(queue)
        consumer.start()

    # Look in the output directory
    today = datetime.date.today()
    for root, dir_names, filenames in os.walk(CHILES_LOGS):
        for match in fnmatch.filter(filenames, '*.log'):
            LOGGER.info('Looking at: {0}'.format(join(root, match)))
            queue.put(CopyTask(join(root, match), '{0}/{1}{2:02d}{3:02d}/{4}'.format(s3_tag, today.year, today.month, today.day, match)))

    for root, dir_names, filenames in os.walk(BENCHMARKING_LOGS):
        for match in fnmatch.filter(filenames, '*.csv'):
            LOGGER.info('Looking at: {0}'.format(join(root, match)))
            queue.put(CopyTask(join(root, match), '{0}/{1}{2:02d}{3:02d}/{4}'.format(s3_tag, today.year, today.month, today.day, match)))
        for match in fnmatch.filter(filenames, '*.log'):
            LOGGER.info('Looking at: {0}'.format(join(root, match)))
            queue.put(CopyTask(join(root, match), '{0}/{1}{2:02d}{3:02d}/{4}'.format(s3_tag, today.year, today.month, today.day, match)))

    queue.put(CopyTask('/var/log/chiles-output.log', '{0}/{1}{2:02d}{3:02d}/chiles-output.log'.format(s3_tag, today.year, today.month, today.day)))

    # Add a poison pill to shut things down
    for x in range(processes):
        queue.put(None)

    # Wait for the queue to terminate
    queue.join()


def main():
    parser = argparse.ArgumentParser('Copy the log files to the correct place in S3')
    parser.add_argument('s3_tag', help='the s3 tag to use')
    parser.add_argument('-p', '--processes', type=int, default=1, help='the number of processes to run')
    args = vars(parser.parse_args())
    s3_tag = args['s3_tag']

    processes = args['processes']

    copy_files(s3_tag, processes)

if __name__ == "__main__":
    main()
