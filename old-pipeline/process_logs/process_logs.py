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
Get the logs and parse them
"""
from datetime import datetime, timedelta
import logging
import os
import time
from s3_helper import S3Helper
from settings_file import CHILES_BUCKET_NAME

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)


def process_cvel():
    s3_helper = S3Helper()
    bucket = s3_helper.get_bucket(CHILES_BUCKET_NAME)
    cvel_data = {}
    for key in bucket.list(prefix='CVEL-logs/'):
        if key.key.endswith('chiles-output.log'):
            elements = key.key.split('/')
            element_1 = elements[1]
            if element_1 == 'standalone':
                # Ignore it
                pass
            elif element_1 not in cvel_data.keys():
                cvel_data[element_1] = elements
            else:
                old_elements = cvel_data[element_1]
                if elements[2] > old_elements[2]:
                    cvel_data[element_1] = elements

    total_time = timedelta()
    for key, value in cvel_data.iteritems():
        new_dir = os.path.join('/tmp', value[0], value[1], value[2], value[3])
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)
        file_name = os.path.join(new_dir, value[4])
        if not os.path.exists(file_name):
            key_name = os.path.join(value[0], value[1], value[2], value[3], value[4])
            s3_helper.get_file_from_bucket(CHILES_BUCKET_NAME, key_name, file_name)

        with open(file_name, 'rb') as file_handle:
            first = next(file_handle)
            offset = -100
            while True:
                file_handle.seek(offset, 2)
                lines = file_handle.readlines()
                if len(lines) > 1:
                    last = lines[-1]
                    break
                offset *= 2

        first_line = first.split()
        last_line = last.split(':')

        start_time = time.strptime('{0} {1} {2}'.format(first_line[0], first_line[1], first_line[2]), '%b %d %H:%M:%S')
        end_time = time.strptime('{0}:{1}:{2}'.format(last_line[1], last_line[2], last_line[3].split(',')[0]), '%Y-%m-%d %H:%M:%S')

        start_time = datetime(end_time.tm_year, start_time.tm_mon, start_time.tm_mday, start_time.tm_hour, start_time.tm_min, start_time.tm_sec)
        end_time = datetime(end_time.tm_year, end_time.tm_mon, end_time.tm_mday, end_time.tm_hour, end_time.tm_min, end_time.tm_sec)

        total_time += end_time - start_time

    return total_time


def process_clean():
    s3_helper = S3Helper()
    bucket = s3_helper.get_bucket(CHILES_BUCKET_NAME)
    clean_data = {}
    for key in bucket.list(prefix='CLEAN-log/'):
        if key.key.endswith('chiles-output.log'):
            elements = key.key.split('/')
            element_1 = elements[1]
            if element_1 == 'standalone':
                # Ignore it
                pass
            elif element_1 not in clean_data.keys():
                clean_data[element_1] = elements
            else:
                old_elements = clean_data[element_1]
                if elements[2] > old_elements[2]:
                    clean_data[element_1] = elements

    total_time = timedelta()
    for key, value in clean_data.iteritems():
        new_dir = os.path.join('/tmp', value[0], value[1], value[2])
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)
        file_name = os.path.join(new_dir, value[3])
        if not os.path.exists(file_name):
            key_name = os.path.join(value[0], value[1], value[2], value[3])
            s3_helper.get_file_from_bucket(CHILES_BUCKET_NAME, key_name, file_name)

        with open(file_name, 'rb') as file_handle:
            first = next(file_handle)
            offset = -100
            while True:
                file_handle.seek(offset, 2)
                lines = file_handle.readlines()
                if len(lines) > 1:
                    last = lines[-1]
                    break
                offset *= 2

        first_line = first.split()
        last_line = last.split(':')

        start_time = time.strptime('{0} {1} {2}'.format(first_line[0], first_line[1], first_line[2]), '%b %d %H:%M:%S')
        end_time = time.strptime('{0}:{1}:{2}'.format(last_line[1], last_line[2], last_line[3].split(',')[0]), '%Y-%m-%d %H:%M:%S')

        start_time = datetime(end_time.tm_year, start_time.tm_mon, start_time.tm_mday, start_time.tm_hour, start_time.tm_min, start_time.tm_sec)
        end_time = datetime(end_time.tm_year, end_time.tm_mon, end_time.tm_mday, end_time.tm_hour, end_time.tm_min, end_time.tm_sec)

        total_time += end_time - start_time

    return total_time


def main():
    total_cvel_time = process_cvel()
    total_clean_time = process_clean()

    LOG.info('Clean: {0}, Cvel: {1}, Total: {2}'.format(total_clean_time, total_cvel_time, total_clean_time + total_cvel_time))


if __name__ == "__main__":
    main()
