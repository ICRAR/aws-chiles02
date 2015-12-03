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
Check the CVEL output
"""
import collections
import logging

from ec2_helper import EC2Helper
from s3_helper import S3Helper
from settings_file import CHILES_BUCKET_NAME, FREQUENCY_GROUPS


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)


def get_cvel(bucket):
    cvel_data_tar = []
    cvel_data_tar_gz = []
    for key in bucket.list(prefix='CVEL/'):
        LOG.info('Checking {0}'.format(key.key))
        if key.key.endswith('data.tar.gz'):
            elements = key.key.split('/')
            cvel_data_tar_gz.append([str(elements[1]), str(elements[2])])
        elif key.key.endswith('data.tar'):
            elements = key.key.split('/')
            cvel_data_tar.append([str(elements[1]), str(elements[2])])

    return cvel_data_tar, cvel_data_tar_gz


def delete_duplicates(tar, tar_gz, bucket):
    deleted = 0
    for tar_entry in tar:
        for tar_gz_entry in tar_gz:
            LOG.info('Checking {0} against {1}'.format(tar_entry, tar_gz_entry))
            if tar_entry[0] == tar_gz_entry[0] and tar_entry[1] == tar_gz_entry[1]:
                LOG.info('Matched {0} against {1}'.format(tar_entry, tar_gz_entry))
                bucket.delete_key('CVEL/{0}/{1}/data.tar.gz'.format(tar_entry[0], tar_entry[1]))
                deleted += 1

    LOG.info('Delete {0} duplicates'.format(deleted))


def main():
    s3_helper = S3Helper()
    bucket = s3_helper.get_bucket(CHILES_BUCKET_NAME)

    # Get the data we need
    tar, tar_gz = get_cvel(bucket)
    delete_duplicates(tar, tar_gz, bucket)


if __name__ == "__main__":
    main()
