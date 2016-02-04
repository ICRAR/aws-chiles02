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

from aws_chiles02.common import run_command

LOG = logging.getLogger(__name__)


def parser_arguments():
    parser = argparse.ArgumentParser('Copy a file from S3 and untar it')
    parser.add_argument('s3_url', help='the s3: url to access')
    parser.add_argument('directory', help='the directory to write the file in')
    parser.add_argument('aws_access_key_id', help="the AWS aws_access_key_id to use")
    parser.add_argument('aws_secret_access_key', help="the AWS aws_secret_access_key to use")

    args = parser.parse_args()
    LOG.info(args)
    return args


def copy_from_s3(args):
    # Does the file exists
    head, tail = os.path.split(args.s3_url)
    measurement_set = os.path.join(args.directory, tail)
    if os.path.exists(measurement_set) and os.path.isdir(measurement_set):
        LOG.info('File: {0} exists'.format(measurement_set))
        return 0

    # Make the directory
    os.makedirs(args.directory)

    # The following will need (16 + 1) * 262144000 bytes of heap space, ie approximately 4.5G.
    # Note setting minimum as well as maximum heap results in OutOfMemory errors at times!
    # The -d64 is to make sure we are using a 64bit JVM.
    bash = 'java -d64 -Xms6g -Xmx6g -classpath /opt/chiles02/aws-chiles02/java/build/awsChiles02.jar org.icrar.awsChiles02.copyS3.CopyFileFromS3' \
           ' -thread_buffer 262144000 -thread_pool 16 -extract_tar -aws_access_key_id {2} -aws_secret_access_key {3}' \
           ' {0} {1}'.format(
                args.s3_url,
                args.directory,
                args.aws_access_key_id,
                args.aws_secret_access_key
            )
    return_code = run_command(bash)

    if return_code != 0 or not os.path.exists(args.measurement_set):
        return 1

    return 0


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    arguments = parser_arguments()
    error_code = copy_from_s3(arguments)
    sys.exit(error_code)
