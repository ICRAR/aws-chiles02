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
Copy the Stats from S3
"""
import os
import threading

import boto3
import logging

import multiprocessing
from boto3.s3.transfer import S3Transfer

from rfi_ml.output_numbers import bytes2human

BUCKET_NAME = '13b-266'
PROCESSES = 4

LOG = None


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


class Consumer(multiprocessing.Process):
    """
    A class to process jobs from the queue
    """
    def __init__(self, queue):
        multiprocessing.Process.__init__(self)
        self._queue = queue

    def run(self):
        """
        Sit in a loop
        """
        while True:
            LOG.info('Getting a task')
            next_task = self._queue.get()
            if next_task is None:
                # Poison pill means shutdown this consumer
                LOG.info('Exiting consumer')
                self._queue.task_done()
                return
            LOG.info('Executing the task')
            # noinspection PyBroadException
            try:
                next_task()
            except:
                LOG.exception('Exception in consumer')
            finally:
                self._queue.task_done()


class CopyData:
    def __init__(self, dir1, filename, key, size):
        self.dir1 = dir1
        self.file = filename
        self.key = key
        self.size = size

    def __call__(self):
        """
        Actually run the job
        """
        tar_file = '/tmp/uvsub_4/{0}/{1}'.format(self.dir1, self.file)
        if os.path.exists(tar_file):
            LOG.info('{0} already exists'.format(tar_file))
        else:
            LOG.info('Copying {0} from S3'.format(tar_file))
            session = boto3.Session(profile_name='aws-chiles02')
            s3 = session.resource('s3', use_ssl=False)
            s3_client = s3.meta.client
            transfer = S3Transfer(s3_client)
            transfer.download_file(
                BUCKET_NAME,
                self.key,
                tar_file,
                callback=ProgressPercentage(
                    self.key,
                    self.size
                )
            )


def copy_stats():
    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)
    bucket = s3.Bucket(BUCKET_NAME)

    # Create the queue
    queue = multiprocessing.JoinableQueue()

    # Start the consumers
    for x in range(PROCESSES):
        consumer = Consumer(queue)
        consumer.start()

    for key in bucket.objects.filter(Prefix='uvsub_4'):
        elements = key.key.split('/')

        if len(elements) == 3 and elements[2].startswith('stats_13') and elements[2].endswith('tar.gz'):
            LOG.info('{0} found'.format(key.key))
            directory_name = '/tmp/uvsub_4/{0}'.format(elements[1])
            if not os.path.exists(directory_name):
                os.makedirs(directory_name)

            queue.put(CopyData(elements[1], elements[2], key.key, key.size))

    # Add a poison pill to shut things down
    for x in range(PROCESSES):
        queue.put(None)

    # Wait for the queue to terminate
    queue.join()


if __name__ == '__main__':
    # TODO: Add parameters
    LOG = multiprocessing.get_logger()
    formatter = logging.Formatter('[%(processName)s]:%(asctime)-15s:%(levelname)s:%(module)s:%(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    LOG.addHandler(handler)
    LOG.propagate = 0
    LOG.setLevel(multiprocessing.SUBDEBUG)

    copy_stats()
