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
Clean up multipart files
"""
import argparse
import logging
import datetime
import boto3

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)


def main():
    parser = argparse.ArgumentParser('Check the multipart upload status')
    parser.add_argument('-c', '--cancel', action="store_true", help='cancel all the outstanding ')
    parser.add_argument('-f', '--force', action="store_true", help='force all the outstanding ')
    parser.add_argument('bucket', help='the bucket to check')
    args = parser.parse_args()

    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)

    bucket = s3.Bucket(args.bucket)

    one_day_ago = datetime.datetime.now() - datetime.timedelta(hours=24)
    for item in bucket.multipart_uploads.all():
        LOG.info('key_name: {0}, initiated: {1}'.format(item.key_name, item.initiated))
        date_initiated = datetime.datetime.strptime(item.initiated, '%Y-%m-%dT%H:%M:%S.%fZ')
        if (date_initiated < one_day_ago and args.cancel) or args.force:
            LOG.info('Cancelling {0}'.format(item.key_name))
            item.abort()


if __name__ == "__main__":
    main()
