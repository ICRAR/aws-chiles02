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

import boto3
import logging

from boto3.s3.transfer import S3Transfer

from aws_chiles02.common import ProgressPercentage

BUCKET_NAME = '13b-266'

LOG = logging.getLogger(__name__)


class CopyData:
    def __init__(self, elements, key, size):
        self.dir1 = elements[1]
        self.file = elements[2]
        self.key = key
        self.size = size


def copy_stats():
    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)
    bucket = s3.Bucket(BUCKET_NAME)
    to_copy = []
    for key in bucket.objects.filter(Prefix='uvsub_4'):
        elements = key.key.split('/')

        if len(elements) == 3 and elements[2].startswith('stats_13') and elements[2].endswith('tar.gz'):
            LOG.info('{0} found'.format(key.key))
            to_copy.append(CopyData(elements, key.key, key.size))

    s3_client = s3.meta.client
    for copy_data in to_copy:
        directory_name = '/tmp/uvsub_4/{0}'.format(copy_data.dir1)
        if not os.path.exists(directory_name):
            os.makedirs(directory_name)

        s3_size = copy_data.size
        transfer = S3Transfer(s3_client)
        transfer.download_file(
            BUCKET_NAME,
            copy_data.key,
            '/tmp/uvsub_4/{0}/{1}'.format(copy_data.dir1, copy_data.file),
            callback=ProgressPercentage(
                copy_data.key,
                s3_size
            )
        )

if __name__ == '__main__':
    # TODO: Add parameters
    logging.basicConfig(level=logging.INFO)

    copy_stats()
