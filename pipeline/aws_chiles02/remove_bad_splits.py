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
Scan the S3 bucket looking for bad splits
"""
import argparse
import logging

import boto3

from .common import human2bytes

LOG = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser('Check what data has been split')
    parser.add_argument('bucket', help='the bucket to access')
    parser.add_argument('width', type=int, help='the frequency width')
    parser.add_argument('size', help='the minimum viable size')
    parser.add_argument('-v', '--verbosity', action='count', default=0, help='increase output verbosity')
    return parser.parse_args()


def remove_bad_splits(s3, bucket_name, width, size):
    bucket = s3.Bucket(bucket_name)
    deleted_count = 0
    checked_count = 0
    for key in bucket.objects.filter(Prefix='split_{0}'.format(width)):
        if key.key.endswith('.tar'):
            checked_count += 1
            s3_object = s3.Object(bucket_name, key.key)
            s3_size = s3_object.content_length

            LOG.debug('checking key: {0}, size: {1:,}'.format(key.key, s3_size))

            if s3_size < size:
                LOG.info('deleting key: {0}, size: {1:,}'.format(key.key, s3_size))
                s3_object.delete()
                deleted_count += 1

    LOG.info('Checked {0} files. Deleted {1} files smaller than {2:,} bytes'.format(checked_count, deleted_count, size))


def main():
    arguments = parse_arguments()
    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)

    size_as_number = human2bytes(arguments.size)
    remove_bad_splits(s3, arguments.bucket, arguments.width, size_as_number)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('nose').setLevel(logging.INFO)

    main()
