"""

"""
import argparse
import logging
from os.path import exists, join, expanduser
import datetime

import boto

from settings_file import CHILES_BUCKET_NAME


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)


def main():
    if exists(join(expanduser('~'), '.aws/credentials')):
        # This relies on a ~/.aws/credentials file holding the '<aws access key>', '<aws secret key>'
        LOG.info("Using ~/.aws/credentials")
        s3_connection = boto.connect_s3(profile_name='chiles')
    else:
        # This relies on a ~/.boto or /etc/boto.cfg file holding the '<aws access key>', '<aws secret key>'
        LOG.info("Using ~/.boto or /etc/boto.cfg")
        s3_connection = boto.connect_s3()

    bucket = s3_connection.get_bucket('testdpstore')

    for item in bucket.list_multipart_uploads():
        LOG.info('key_name: {0}, initiated: {1}'.format(item.key_name, item.initiated))
        bucket.cancel_multipart_upload(item.key_name, item.id)


if __name__ == "__main__":
    main()
