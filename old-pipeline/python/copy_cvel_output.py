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
Copy the CVEL output files to S3
"""
import argparse
import fnmatch
import os
from os.path import join
import shutil
import sys

from common import make_safe_filename, LOGGER, make_tarfile, can_be_multipart_tar
from echo import echo
from settings_file import CHILES_CVEL_OUTPUT, CHILES_BUCKET_NAME
from s3_helper import S3Helper


LOGGER.info('PYTHONPATH = {0}'.format(sys.path))


@echo
def copy_files(date, vis_file):
    s3_helper = S3Helper()
    # Look in the output directory
    for root, dir_names, filenames in os.walk(CHILES_CVEL_OUTPUT):
        LOGGER.info('root: {0}, dir_names: {1}, filenames: {2}'.format(root, dir_names, filenames))
        for match in fnmatch.filter(dir_names, vis_file):
            result_dir = join(root, match)
            LOGGER.info('Working on: {0}'.format(result_dir))

            if can_be_multipart_tar(result_dir):
                LOGGER.info('Using add_tar_to_bucket_multipart')
                s3_helper.add_tar_to_bucket_multipart(
                    CHILES_BUCKET_NAME,
                    'CVEL/{0}/{1}/data.tar'.format(vis_file, date),
                    result_dir)
            else:
                LOGGER.info('Using make_tarfile, then adding file to bucket')
                output_tar_filename = join(root, match + '.tar')
                make_tarfile(output_tar_filename, result_dir)

                s3_helper.add_file_to_bucket(
                    CHILES_BUCKET_NAME,
                    'CVEL/{0}/{1}/data.tar'.format(vis_file, date),
                    output_tar_filename)

                # Clean up
                os.remove(output_tar_filename)

            shutil.rmtree(result_dir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser('Copy the CVEL output to the correct place in S3')
    parser.add_argument('vis_file', help='the visibilites file')
    parser.add_argument('date', help='the date of the observation')
    args = vars(parser.parse_args())
    date = make_safe_filename(args['date'])
    vis_file = args['vis_file']

    copy_files(date, vis_file)

if __name__ == "__main__":
    main()
