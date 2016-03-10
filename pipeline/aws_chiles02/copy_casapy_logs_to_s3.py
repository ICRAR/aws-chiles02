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
Copy files to S3
"""
import logging
import argparse
import os
import sys

from aws_chiles02.common import run_command

LOG = logging.getLogger(__name__)


def parser_arguments():
    parser = argparse.ArgumentParser('Copy a file from S3 and untar it')
    parser.add_argument('directory', help='the directory to get the log files from')
    parser.add_argument('s3_url', help='the s3: url to access')

    args = parser.parse_args()
    LOG.info(args)
    return args


def copy_logs_to_s3(args):
    # Make the tar file
    tar_filename = os.path.join(args.directory, 'logs.tar')
    os.chdir(args.directory)
    bash = 'tar -cvf {0} *.log'.format(tar_filename)
    return_code = run_command(bash)
    path_exists = os.path.exists(tar_filename)
    if return_code != 0 or not path_exists:
        LOG.error('tar return_code: {0}, exists: {1}'.format(return_code, path_exists))
        return 0

    else:
        bash = 'java -classpath /opt/chiles02/aws-chiles02/java/build/awsChiles02.jar org.icrar.awsChiles02.copyS3.CopyFileToS3' \
               ' -aws_profile aws-chiles02 {0} {1}'.format(
                args.s3_url,
                tar_filename
        )
        return_code = run_command(bash)

        return return_code


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    arguments = parser_arguments()
    error_code = copy_logs_to_s3(arguments)
    sys.exit(error_code)
