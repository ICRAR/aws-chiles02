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
Move the data from S3
"""
import argparse
import logging
import os
import subprocess
import threading
import time
from cStringIO import StringIO
from os import makedirs, remove, rename
from os.path import basename, exists, getsize, join, splitext

import boto3
from s3transfer import S3Transfer

LOG = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('bucket_name', help='the bucket name')
    parser.add_argument('folder_name', help='the folder in the bucket with the data')
    parser.add_argument('output_file', help='the file to put the data in')
    return parser.parse_args()


def build_file(bucket_name, folder_name, output_file):
    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)
    bucket = s3.Bucket(bucket_name)
    with open(output_file, "w") as output_file:
        for key in bucket.objects.filter(Prefix='{0}'.format(folder_name)):
            if key.key.endswith('.tar'):
                output_file.write(key.key + '\n')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    LOG.info(args)
    build_file(args.bucket_name, args.folder_name, args.output_file)
