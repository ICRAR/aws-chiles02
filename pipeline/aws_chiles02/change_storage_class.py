#
#    Copyright (c) UWA, The University of Western Australia
#    M468/35 Stirling Hwy
#    Perth WA 6009
#    Australia
#
#    All rights reserved 2019
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
import logging
import multiprocessing

import boto3


LOGGER = logging.getLogger(__name__)


class Consumer(multiprocessing.Process):
    def __init__(self, task_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue

    def run(self):
        proc_name = self.name
        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)
        while True:
            next_task = self.task_queue.get()
            if next_task is None:
                # Poison pill means shutdown
                print('{}: Exiting'.format(proc_name))
                self.task_queue.task_done()
                break
            LOGGER.info('{}: {}'.format(proc_name, next_task))
            next_task(s3)
            self.task_queue.task_done()


class Task(object):
    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key

    def __call__(self, s3):
        LOGGER.info('Staring copy: {}'.format(self.key))

        object_ = s3.Object(self.bucket, self.key)
        if object_.storage_class != 'INTELLIGENT_TIERING':
            object_.copy_from(
                CopySource={'Bucket': self.bucket, 'Key': self.key},
                StorageClass='INTELLIGENT_TIERING'
            )

            LOGGER.info('Copy complete: {}'.format(object_))
        else:
            LOGGER.info('Already in the correct storage format')

    def __str__(self):
        return '{}:{}'.format(self.bucket, self.key)


def parse_arguments():
    parser = argparse.ArgumentParser('S3 objects to move')
    parser.add_argument('bucket', help='the bucket to access')
    parser.add_argument('prefixes_to_copy', nargs=argparse.ONE_OR_MORE)
    parser.add_argument('-d', '--dry_run', action='store_true', help='a dry run')
    parser.add_argument('-p', '--processes', type=int, default=8, help='a dry run')
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    arguments = parse_arguments()

    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)
    bucket = s3.Bucket(arguments.bucket)

    keys = []
    for prefix in arguments.prefixes_to_copy:
        for object_ in bucket.objects.filter(Prefix=prefix):
            if not object_.key.endswith('/'):
                keys.append(object_.key)

    if arguments.dry_run:
        for key in keys:
            LOGGER.info('Dry run: {}'.format(key))
    else:
        tasks = multiprocessing.JoinableQueue()
        LOGGER.info('Creating {} consumers'.format(arguments.processes))
        consumers = [Consumer(tasks) for _ in range(arguments.processes)]
        for w in consumers:
            w.start()

        # Enqueue jobs
        for key in keys:
            tasks.put(Task(arguments.bucket, key))

        # Add a poison pill for each consumer
        for i in range(arguments.processes):
            tasks.put(None)

        # Wait for all of the tasks to finish
        tasks.join()


if __name__ == "__main__":
    main()
