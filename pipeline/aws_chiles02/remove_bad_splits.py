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

LOG = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser('Check what data has been split')
    parser.add_argument('bucket', help='the bucket to access')
    parser.add_argument('width', type=int, help='the frequency width')
    parser.add_argument('size', type=int, help='the minimum viable size')
    return parser.parse_args()


def remove_bad_splits(s3, bucket_name, width, size):
    bucket = s3.Bucket(bucket_name)
    for key in bucket.objects.filter(Prefix='split_{0}'.format(width)):
        if key.key.endswith('.tar'):
            s3_object = s3.Object(bucket_name, key.key)
            s3_size = s3_object.content_length

            LOG.debug('checking key: {0}, size: {1:,}'.format(key.key, s3_size))

            if s3_size < size:
                LOG.info('deleting key: {0}, size: {1:,}'.format(key.key, s3_size))
                s3_object.delete()


def main():
    arguments = parse_arguments()
    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)

    remove_bad_splits(s3, arguments.bucket, arguments.width, arguments.size)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('boto3').setLevel(logging.INFO)
    logging.getLogger('botocore').setLevel(logging.INFO)
    logging.getLogger('nose').setLevel(logging.INFO)

    main()
