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

"""
import fnmatch
import logging
import argparse
import os
from os import walk
from os.path import exists, isdir, join, split

import boto3
from boto3.s3.transfer import S3Transfer

from aws_chiles02.common import run_command, ProgressPercentage

LOG = logging.getLogger(__name__)
logging.getLogger('boto3').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('nose').setLevel(logging.INFO)


def copy_measurement_set(measurement_set, directory_out, bucket_name):
    LOG.info('measurement_set: {0}, bucket_name: {1}'.format(measurement_set, bucket_name))

    (measurement_set_directory, measurement_set_filename) = split(measurement_set)
    key = 'observation_data/{0}.tar'.format(measurement_set_filename)

    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)

    bucket = s3.Bucket(bucket_name)
    objs = list(bucket.objects.filter(Prefix=key))
    if len(objs) > 0 and objs[0].key == key:
        LOG.info('The measurement set {0} exists in {1}'.format(key, bucket_name))
    else:
        tar_filename = os.path.join(directory_out, '{0}.tar'.format(measurement_set_filename))
        bash = 'cd {0} && tar -cvf {1} {2}'.format(measurement_set_directory, tar_filename, measurement_set_filename)
        return_code = run_command(bash)
        path_exists = os.path.exists(tar_filename)
        if return_code != 0 or not path_exists:
            LOG.error('tar return_code: {0}, exists: {1}'.format(return_code, path_exists))
        else:
            session = boto3.Session(profile_name='aws-chiles02')
            s3 = session.resource('s3', use_ssl=False)

            s3_client = s3.meta.client
            transfer = S3Transfer(s3_client)
            transfer.upload_file(
                tar_filename,
                bucket_name,
                key,
                callback=ProgressPercentage(
                    key,
                    float(os.path.getsize(tar_filename))
                ),
                extra_args={
                    'StorageClass': 'REDUCED_REDUNDANCY',
                }
            )

        # Clean up
        if path_exists:
            os.remove(tar_filename)


def write_files(list_measurement_sets, directory_out, bucket_name):
    for measurement_set in list_measurement_sets:
        copy_measurement_set(measurement_set, directory_out, bucket_name)


def get_list_measurement_sets(directory_in):
    LOG.info('Starting at: {0}'.format(directory_in))
    list_measurement_sets = []
    for root, dir_names, filenames in walk(directory_in):
        for match in fnmatch.filter(dir_names, '*_calibrated_deepfield.ms'):
            measurement_set = join(root, match)
            LOG.info('Looking at: {0}'.format(measurement_set))
            list_measurement_sets.append(measurement_set)

    return list_measurement_sets


def copy_measurement_sets(args):
    if not exists(args.directory_in) or not isdir(args.directory_in):
        LOG.error('The input directory {0} does not exist'.format(args.directory_in))
    if not exists(args.directory_out) or not isdir(args.directory_out):
        LOG.error('The output directory {0} does not exist'.format(args.directory_out))

    list_measurement_sets = get_list_measurement_sets(args.directory_in)
    write_files(list_measurement_sets, args.directory_out, args.bucket_name)


def main():
    parser = argparse.ArgumentParser('Get files and move them to S3')
    parser.add_argument('directory_in', help='the input directory to scan')
    parser.add_argument('directory_out', help='the output directory to write the tar files to')
    parser.add_argument('bucket_name', help='where to write the files')
    args = parser.parse_args()

    copy_measurement_sets(args)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
