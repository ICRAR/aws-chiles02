#
#    Copyright (c) UWA, The University of Western Australia
#    M468/35 Stirling Hwy
#    Perth WA 6009
#    Australia
#
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
A helper for S3
"""
from os.path import join, expanduser, exists
import socket
import ssl
import tarfile
import time
import math
import mimetypes
from multiprocessing import Pool
import os
from cStringIO import StringIO

import boto
from boto.s3.key import Key

from common import LOGGER
from file_chunk_io import FileChunkIO


class S3UploadException(Exception):
    """
    Something went wrong with the S3 upload
    """
    def __init__(self, error):
        self.error = error

# There is a bug in BOTO at the moment
if hasattr(ssl, '_create_unverified_context'):
    ssl._create_default_https_context = ssl._create_unverified_context


def get_s3_connection(aws_access_key_id=None, aws_secret_access_key=None):
    if aws_access_key_id is not None and aws_secret_access_key is not None:
        LOGGER.info("Using user provided keys")
        return boto.connect_s3(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key)
    elif exists(join(expanduser('~'), '.aws/credentials')):
        # This relies on a ~/.aws/credentials file holding the '<aws access key>', '<aws secret key>'
        LOGGER.info("Using ~/.aws/credentials")
        return boto.connect_s3(profile_name='chiles')
    else:
        # This relies on a ~/.boto or /etc/boto.cfg file holding the '<aws access key>', '<aws secret key>'
        LOGGER.info("Using ~/.boto or /etc/boto.cfg")
        return boto.connect_s3()


def upload_part(aws_access_key_id, aws_secret_access_key, bucket_name, multipart_id, part_num, source_path, offset, bytes_to_copy, amount_of_retries=10):
    """
    Uploads a part with retries.  It is outside the class to get pickling to work properly
    """
    def _upload(retries_left=amount_of_retries):
        try:
            LOGGER.info(
                'Start uploading part: #{0}, source_path: {1}'.format(
                    part_num,
                    source_path
                )
            )
            conn = get_s3_connection(aws_access_key_id, aws_secret_access_key)
            bucket = conn.get_bucket(bucket_name)
            for mp in bucket.get_all_multipart_uploads():
                if mp.id == multipart_id:
                    with FileChunkIO(source_path, 'r', offset=offset, bytes=bytes_to_copy) as fp:
                        mp.upload_part_from_file(fp=fp, part_num=part_num)
                    break
        except Exception, exc:
            if retries_left:
                _upload(retries_left=retries_left - 1)
            else:
                LOGGER.info(
                    'Failed uploading part: #{0}, source_path: {1}'.format(
                        part_num,
                        source_path
                    )
                )
                raise exc
        else:
            LOGGER.info(
                'Uploaded part part: #{0}, source_path: {1}'.format(
                    part_num,
                    source_path
                )
            )

    _upload()


def upload_part_stream(aws_access_key_id, aws_secret_access_key, bucket_name, multipart_id, part_num, data_to_store, amount_of_retries=10):
    """
    Uploads a part with retries.  It is outside the class to get pickling to work properly
    """
    def _upload(retries_left=amount_of_retries):
        try:
            LOGGER.info('Start uploading part #{0} ...'.format(part_num))
            conn = get_s3_connection(aws_access_key_id, aws_secret_access_key)
            bucket = conn.get_bucket(bucket_name)
            for mp in bucket.get_all_multipart_uploads():
                if mp.id == multipart_id:
                    fp = StringIO(data_to_store)
                    mp.upload_part_from_file(fp=fp, part_num=part_num, replace=True)
                    fp.close()          # Tidy up after ourselves
                    break
        except Exception, exc:
            if retries_left:
                LOGGER.exception('... Failed uploading part #{0} retries left {1}'.format(part_num, retries_left))
                _upload(retries_left=retries_left - 1)
            else:
                LOGGER.exception('... Failed uploading part #{0}'.format(part_num))
                raise exc
        else:
            LOGGER.info('... Uploaded part #{0}'.format(part_num))

    _upload()


class S3Helper:
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None):
        """
        Get an S3 connection
        :return:
        """
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self.s3_connection = get_s3_connection(aws_access_key_id, aws_secret_access_key)

    def get_bucket(self, bucket_name):
        """
        Get a S3 bucket

        :param bucket_name:
        :return:
        """
        return self.s3_connection.get_bucket(bucket_name)

    def get_file_from_bucket(self, bucket_name, key_name, file_name):
        """
        Get a file from S3 into a local file

        :param bucket_name:
        :param key_name:
        :param file_name:
        :return:
        """
        bucket = self.get_bucket(bucket_name)
        key = Key(bucket)
        key.key = key_name
        key.get_contents_to_filename(file_name)

    def add_file_to_bucket(self, bucket_name, key_name, filename, reduced_redundancy=True):
        """
        Add file to a bucket

        :param bucket_name:
        :param key_name:
        :param filename:
        """
        LOGGER.info('bucket_name: {0}, key_name: {1}, filename: {2}, reduced_redundancy: {3}'.format(bucket_name, key_name, filename, reduced_redundancy))
        retry_count = 0
        done = False
        while retry_count < 3 and not done:
            try:
                bucket = self.get_bucket(bucket_name)
                key = Key(bucket)
                key.key = key_name
                key.set_contents_from_filename(filename, reduced_redundancy=reduced_redundancy)
                done = True
            except socket.error:
                LOGGER.exception('Error')
                retry_count += 1
                time.sleep(10)

    def add_file_to_bucket_multipart(self, bucket_name, key_name, source_path, parallel_processes=2, reduced_redundancy=True):
        """
        Parallel multipart upload.
        """
        LOGGER.info('bucket_name: {0}, key_name: {1}, filename: {2}, parallel_processes: {3}, reduced_redundancy: {4}'.format(
            bucket_name, key_name, source_path, parallel_processes, reduced_redundancy))

        source_size = os.stat(source_path).st_size
        bytes_per_chunk = 10 * 1024 * 1024
        chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))
        if chunk_amount < 10000:
            bucket = self.get_bucket(bucket_name)

            headers = {'Content-Type': mimetypes.guess_type(key_name)[0] or 'application/octet-stream'}
            mp = bucket.initiate_multipart_upload(key_name, headers=headers, reduced_redundancy=reduced_redundancy)

            LOGGER.info('bytes_per_chunk: {0}, chunk_amount: {1}'.format(bytes_per_chunk, chunk_amount))

            # You can only upload 10,000 chunks
            pool = Pool(processes=parallel_processes)
            for i in range(chunk_amount):
                offset = i * bytes_per_chunk
                remaining_bytes = source_size - offset
                bytes_to_copy = min([bytes_per_chunk, remaining_bytes])
                part_num = i + 1
                pool.apply_async(upload_part, [self._aws_access_key_id, self._aws_secret_access_key, bucket_name, mp.id, part_num, source_path, offset, bytes_to_copy])
            pool.close()
            pool.join()

            if len(mp.get_all_parts()) == chunk_amount:
                mp.complete_upload()
            else:
                mp.cancel_upload()
        else:
            raise S3UploadException('Too many chunks')

    def add_tar_to_bucket_multipart(self, bucket_name, key_name, source_path, gzip=False, parallel_processes=2, reduced_redundancy=True, bufsize=10*1024*1024):
        """
        Parallel multipart upload.
        """
        LOGGER.info(
            'bucket_name: {0}, key_name: {1}, source_path: {2}, parallel_processes: {3}, reduced_redundancy: {4}, bufsize: {5}'.format(
                bucket_name,
                key_name,
                source_path,
                parallel_processes,
                reduced_redundancy,
                bufsize
            )
        )
        bucket = self.get_bucket(bucket_name)

        headers = {'Content-Type': mimetypes.guess_type(key_name)[0] or 'application/octet-stream'}
        mp = bucket.initiate_multipart_upload(key_name, headers=headers, reduced_redundancy=reduced_redundancy)
        s3_feeder = S3Feeder(mp.id, bufsize, bucket_name, parallel_processes, self._aws_access_key_id, self._aws_secret_access_key)

        if gzip:
            mode = "w|gz"
        else:
            mode = "w|"
        tar = tarfile.open(mode=mode, fileobj=s3_feeder, bufsize=int(bufsize / 10))

        complete = True
        # noinspection PyBroadException
        try:
            for entry in os.listdir(source_path):
                full_filename = join(source_path, entry)
                LOGGER.info(
                    'tar: [full_filename: {0}, entry: {1}]'.format(
                        full_filename,
                        entry
                    )
                )
                tar.add(full_filename, arcname=entry)

            tar.close()
            s3_feeder.close()
        except Exception:
            complete = False
            s3_feeder.close()

        # Finish the upload
        if complete:
            mp.complete_upload()
        else:
            mp.cancel_upload()


class S3Feeder:
    def __init__(self, multipart_upload_id, max_buffer_size, bucket_name, parallel_processes, aws_access_key_id=None, aws_secret_access_key=None):
        self._file_str = StringIO()
        self._buffer_length = 0
        self._part_num = 1
        self._multipart_upload_id = multipart_upload_id
        self._max_buffer_size = max_buffer_size
        self._bucket_name = bucket_name
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._closed = False
        self._pool = Pool(processes=parallel_processes)

    def write(self, data):
        self._file_str.write(data)
        self._buffer_length += len(data)
        if self._buffer_length > self._max_buffer_size:
            if self._part_num <= 10000:
                self._pool.apply_async(
                    upload_part_stream,
                    [self._aws_access_key_id, self._aws_secret_access_key, self._bucket_name, self._multipart_upload_id, self._part_num, self._file_str.getvalue()])
                self._file_str.close()
                self._part_num += 1
                self._file_str = StringIO()
                self._buffer_length = 0
            else:
                raise S3UploadException('Too many chunks')

    def close(self):
        if not self._closed:
            if self._part_num <= 10000:
                self._pool.apply_async(
                    upload_part_stream,
                    [self._aws_access_key_id, self._aws_secret_access_key, self._bucket_name, self._multipart_upload_id, self._part_num, self._file_str.getvalue()])
                self._file_str.close()

                # Allow the last process to start
                time.sleep(0.5)
                self._pool.close()
                self._pool.join()
                self._closed = True
            else:
                raise S3UploadException('Too many chunks')
