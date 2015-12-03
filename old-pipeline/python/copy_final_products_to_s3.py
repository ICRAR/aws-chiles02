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
Copy the final products to S3
"""
import argparse
import fnmatch
import os
from os.path import join
import multiprocessing
import sys
from common import Consumer, LOGGER
from s3_helper import S3Helper

PROCESSES = 4
LOGGER.info('PYTHONPATH = {0}'.format(sys.path))


class CopyTask(object):
    def __init__(self, bucket, bucket_location, filename, aws_access_key_id, aws_secret_access_key):
        self._bucket = bucket
        self._bucket_location = bucket_location
        self._filename = filename
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key

    def __call__(self):
        # noinspection PyBroadException
        try:
            s3_helper = S3Helper(self._aws_access_key_id, self._aws_secret_access_key)
            LOGGER.info('Copying to: {0}/{1}/measurement_set.tar'.format(self._bucket, self._bucket_location))

            # We can have 10,000 parts
            # The biggest file from Semester 1 is 803GB
            # So 100 MB
            s3_helper.add_tar_to_bucket_multipart(
                self._bucket,
                '{0}/measurement_set.tar'.format(self._bucket_location),
                self._filename,
                parallel_processes=2,
                bufsize=100*1024*1024
            )
        except Exception:
            LOGGER.exception('CopyTask died')


def copy_files(args):
    # Create the queue
    queue = multiprocessing.JoinableQueue()
    # Start the consumers
    for x in range(PROCESSES):
        consumer = Consumer(queue)
        consumer.start()

    # Look in the output directory
    for root, dir_names, filenames in os.walk(args.product_dir):
        LOGGER.debug('root: {0}, dir_names: {1}, filenames: {2}'.format(root, dir_names, filenames))
        for match in fnmatch.filter(dir_names, '13B-266*calibrated_deepfield.ms'):
            result_dir = join(root, match)
            LOGGER.info('Queuing result_dir: {0}'.format(result_dir))

            queue.put(
                CopyTask(
                    args.bucket,
                    match,
                    result_dir,
                    args.aws_access_key_id,
                    args.aws_secret_access_key
                )
            )

    # Add a poison pill to shut things down
    for x in range(PROCESSES):
        queue.put(None)

    # Wait for the queue to terminate
    queue.join()


def main():
    parser = argparse.ArgumentParser('Move the final products to S3')
    parser.add_argument('product_dir', help='where the date is stored')
    parser.add_argument('bucket', help='the bucket to use')
    parser.add_argument('aws_access_key_id', help='the AWS access key')
    parser.add_argument('aws_secret_access_key', help='the AWS secret access key')
    args = parser.parse_args()

    copy_files(args)


if __name__ == "__main__":
    main()
