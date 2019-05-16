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


LOGGER = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser('S3 objects to move')
    parser.add_argument('bucket', help='the bucket to access')
    parser.add_argument('prefixes_to_copy', nargs=argparse.ONE_OR_MORE)
    parser.add_argument('-d', '--dry_run', action='store_true', help='a dry run')
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)
    arguments = parse_arguments()

    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)
    bucket = s3.Bucket(arguments.bucket)

    keys = []
    for prefix in arguments.prefixes_to_copy:
        for object_ in bucket.objects.filter(Prefix=prefix):
            if not object_.key.endswith('/'):
                keys.append(object_.key)

    for key in keys:
        if arguments.dry_run:
            LOGGER.info('Dry run: {}'.format(key))
        else:
            LOGGER.info('Staring copy: {}'.format(key))

            object_ = s3.Object(arguments.bucket, key)
            if object_.storage_class != 'INTELLIGENT_TIERING':
                object_.copy_from(
                    CopySource={'Bucket': arguments.bucket, 'Key': key},
                    StorageClass='INTELLIGENT_TIERING'
                )

                LOGGER.info('Copy complete: {}'.format(object_))


if __name__ == "__main__":
    main()
