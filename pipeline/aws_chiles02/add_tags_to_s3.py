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

import boto3

from aws_chiles02.common import set_logging_level

LOGGER = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser('S3 objects to tag')
    parser.add_argument('bucket', help='the bucket to access')
    parser.add_argument('prefixes_to_tag')
    parser.add_argument('tag')
    parser.add_argument('value')
    parser.add_argument('-d', '--dry_run', action='store_true', help='a dry run')
    return parser.parse_args()


def main():
    set_logging_level(0)
    arguments = parse_arguments()

    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)
    bucket = s3.Bucket(arguments.bucket)

    for prefix in arguments.prefixes_to_tag:
        for object_ in bucket.objects.filter(Prefix=prefix):
            if not object_.key.endswith('/'):
                if arguments.dry_run:
                    LOGGER.info('Dry run tagging: {} with {}:{}'.format(object_, arguments.tag, arguments.value))
                else:
                    LOGGER.info('Tagging: {} with {}:{}'.format(object_, arguments.tag, arguments.value))
                    object_.put(
                        Tagging='{}={}'.format(arguments.tag, arguments.value)
                    )
                    LOGGER.info('Tagging complete: {}'.format(object_))


if __name__ == "__main__":
    main()
