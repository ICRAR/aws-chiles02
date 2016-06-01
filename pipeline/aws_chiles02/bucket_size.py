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
Calculate the size of a bucket
"""
import argparse
import logging

import boto3

from aws_chiles02.common import bytes2human, set_boto_logging_level

LOG = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser('How much data is stored in a bucket')
    parser.add_argument('bucket', help='the bucket to access')
    parser.add_argument('-v', '--verbosity', action='count', default=0, help='increase output verbosity')
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)
    arguments = parse_arguments()
    if arguments.verbosity == 0:
        set_boto_logging_level(level=logging.WARN)
    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)

    bucket = s3.Bucket(arguments.bucket)
    size = 0
    count = 0
    for key in bucket.objects.all():
        size += key.size
        count += 1

        if count % 100 == 0:
            LOG.info('Count: {0}, size: {1}'.format(count, bytes2human(size)))

    LOG.info('Size: {0}'.format(bytes2human(size)))


if __name__ == "__main__":
    main()
