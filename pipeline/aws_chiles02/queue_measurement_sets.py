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
Retrieve measurement sets from glacier
"""
import argparse
import logging

import boto3

LOG = logging.getLogger(__name__)

_1GB = 1073741824


def parser_arguments():
    parser = argparse.ArgumentParser('Copy files from Glacier back to S3')
    parser.add_argument('bucket', help='the s3 bucket')

    args = parser.parse_args()
    LOG.info(args)
    return args


def retrieve_files(args):
    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)

    bucket = s3.Bucket(args.bucket)
    for key in bucket.objects.all():
        if key.key.endswith('_calibrated_deepfield.ms.tar'):
            obj = s3.Object(key.bucket_name, key.key)
            storage_class = obj.storage_class
            restore = obj.restore
            LOG.info('{0}, {1}, {2}'.format(key.key, storage_class, restore))

            ok_to_queue = True
            if 'GLACIER' == storage_class:
                if restore is None or restore.startswith('ongoing-request="true"'):
                    ok_to_queue = False

            if ok_to_queue:
                if key.size <= 300 * _1GB:
                    queue = 'xlarge'
                elif key.size <= 600 * _1GB:
                    queue = '2xlarge'
                else:
                    queue = '4xlarge'

                LOG.info('{0}, {1}, {2}'.format(key.key, key.size, queue))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    arguments = parser_arguments()
    retrieve_files(arguments)
