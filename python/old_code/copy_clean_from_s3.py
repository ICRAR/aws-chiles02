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
Copy files from S3
"""
import argparse
import logging
import os
import sys

import boto3

from aws_chiles02.common import run_command, split_s3_url

LOG = logging.getLogger(__name__)
TAR_FILE = 'ms.tar'


def parser_arguments():
    parser = argparse.ArgumentParser('Copy a file from S3 and untar it')
    parser.add_argument('s3_url', help='the s3: url to access')
    parser.add_argument('directory', help='the directory to write the file in')
    parser.add_argument('min_frequency', help="the min frequency to use")
    parser.add_argument('max_frequency', help="the max frequency to use")

    args = parser.parse_args()
    LOG.info(args)
    return args


def get_s3_size(s3_url):
    """
    Get the size of an s3 object
    :param s3_url:
    :return:
    >>> get_s3_size('s3://13b-266/13B-266.sb28624226.eb28625769.56669.43262586805_calibrated_deepfield.ms.tar')
    46726881280
    """
    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)

    bucket_name, key = split_s3_url(s3_url)
    s3_object = s3.Object(bucket_name, key)
    return s3_object.content_length


def copy_from_s3(args):
    # Does the file exists
    measurement_set = os.path.join(args.directory, 'vis_{0}~{1}'.format(args.min_frequency, args.max_frequency))
    LOG.info('Checking {0} exists'.format(measurement_set))
    if os.path.exists(measurement_set) and os.path.isdir(measurement_set):
        LOG.info('Measurement Set: {0} exists'.format(measurement_set))
        return 0

    # Make the directory
    if not os.path.exists(args.directory):
        os.makedirs(args.directory)

    # The following will need (16 + 1) * 262144000 bytes of heap space, ie approximately 4.5G.
    # Note setting minimum as well as maximum heap results in OutOfMemory errors at times!
    # The -d64 is to make sure we are using a 64bit JVM.
    # When extracting to the tar we need even more
    full_path_tar_file = os.path.join(args.directory, TAR_FILE)
    LOG.info('Tar: {0}'.format(full_path_tar_file))
    bash = 'java -d64 -Xms10g -Xmx10g -classpath /opt/chiles02/aws-chiles02/java/build/awsChiles02.jar org.icrar.awsChiles02.copyS3.CopyFileFromS3' \
           ' -thread_buffer 262144000 -thread_pool 16 -aws_profile aws-chiles02' \
           ' {0} {1}'.format(
                args.s3_url,
                full_path_tar_file,
            )
    run_command(bash)

    if not os.path.exists(full_path_tar_file):
        LOG.error('The tar file {0} does not exist'.format(full_path_tar_file))
        return 1

    # Check the sizes match
    s3_size = get_s3_size(args.s3_url)
    tar_size = os.path.getsize(full_path_tar_file)
    if s3_size != tar_size:
        LOG.error('The sizes for {0} differ S3: {1}, local FS: {2}'.format(full_path_tar_file, s3_size, tar_size))
        return 1

    # The tar file exists and is the same size
    bash = 'tar -xvf {0} -C {1}'.format(full_path_tar_file, args.directory)
    return_code = run_command(bash)

    path_exists = os.path.exists(measurement_set)
    if return_code != 0 or not path_exists:
        LOG.error('tar return_code: {0}, exists: {1}-{2}'.format(return_code, measurement_set, path_exists))
        return 1

    os.remove(full_path_tar_file)

    return 0


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    arguments = parser_arguments()
    error_code = copy_from_s3(arguments)
    sys.exit(error_code)
