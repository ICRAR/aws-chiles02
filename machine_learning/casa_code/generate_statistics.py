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

"""
import argparse
import shutil
import time
from os import remove
from os.path import basename, getsize, splitext, join, exists

import boto3
from s3transfer import S3Transfer
from sqlalchemy import create_engine

from casa_code.casa_logging import CasaLogger, echo
from casa_code.common import ProgressPercentage, run_command, stopwatch
from casa_code.database import DATABASE_PATH, METADATA, OBSERVATION, SCAN, SQLITE
from casa_code.get_statistics import GetStatistics

LOG = CasaLogger(__name__)
TAR_FILE = '/mnt/data/tar_file.tar'
MEASUREMENT_SET_DIR = '/mnt/data/measurement_set'


class GenerateStatisticsException(Exception):
    pass


class GenerateStatistics(object):
    def __init__(self, bucket_name, folder_name):
        self._bucket_name = bucket_name
        self._folder_name = folder_name

        self._connection = None
        self._map_observations = {}
        self._measurement_sets = []
        self._s3 = None
        self._insert_scan = SCAN.insert()
        self._insert_observation = OBSERVATION.insert()

    def create_database(self):
        engine = create_engine(SQLITE + DATABASE_PATH)

        # Create the database and import data
        METADATA.create_all(engine)
        self._connection = engine.connect()

    def get_list_measurement_sets(self):
        session = boto3.Session(profile_name='aws-chiles02')
        self._s3 = session.resource('s3', use_ssl=False)

        bucket = self._s3.Bucket(self._bucket_name)

        for key in bucket.objects.filter(Prefix='{0}'.format(self._folder_name)):
            self._measurement_sets.append(key.key)

    def run(self):
        self.create_database()
        self.get_list_measurement_sets()

        for tarred_measurement_set in self._measurement_sets:
            with stopwatch('Copy from S3'):
                measurement_set = self.copy_from_s3(tarred_measurement_set)
            with stopwatch('Add to database'):
                observation_name = self.get_observation_name(tarred_measurement_set)
                self.add_to_database(measurement_set, observation_name)
            self.delete_processed_data(measurement_set)

        self.copy_to_s3()

    @echo
    def copy_from_s3(self, measurement_set):
        s3_object = self._s3.Object(self._bucket_name, measurement_set)
        s3_size = s3_object.content_length
        s3_client = self._s3.meta.client
        transfer = S3Transfer(s3_client)
        transfer.download_file(
            self._bucket_name,
            measurement_set,
            TAR_FILE,
            callback=ProgressPercentage(
                measurement_set,
                s3_size
            )
        )
        if not exists(TAR_FILE):
            message = 'The tar file {0} does not exist'.format(TAR_FILE)
            raise GenerateStatisticsException(message)

        # Check the sizes match
        tar_size = getsize(TAR_FILE)
        if s3_size != tar_size:
            message = 'The sizes for {0} differ S3: {1}, local FS: {2}'.format(TAR_FILE, s3_size, tar_size)
            raise GenerateStatisticsException(message)

        # The tar file exists and is the same size
        bash = 'tar -xvf {0} -C {1}'.format(TAR_FILE, MEASUREMENT_SET_DIR)
        return_code = run_command(bash)

        elements = measurement_set.split('/')
        elements = elements[1].split('_')
        measurement_set_path = join(MEASUREMENT_SET_DIR, 'uvsub_{0}~{1}'.format(elements[0], elements[1]))
        path_exists = exists(measurement_set_path)
        if return_code != 0 or not path_exists:
            message = 'tar return_code: {0}, exists: {1}-{2}'.format(return_code, measurement_set_path, path_exists)
            raise GenerateStatisticsException(message)

        return measurement_set_path

    @echo
    def add_to_database(self, measurement_set, observation_name):
        transaction = self._connection.begin()
        observation_id = self.get_observation_id(observation_name)
        get_statistics = GetStatistics(measurement_set)
        get_statistics.extract_statistics(self, observation_id)
        transaction.commit()

    @staticmethod
    def delete_processed_data(measurement_set):
        remove(TAR_FILE)
        shutil.rmtree(measurement_set)

    def copy_to_s3(self):
        key = 'statistics/{0}'.format(time.strftime('%Y%m%d%H%M%S'))
        s3_client = self._s3.meta.client
        transfer = S3Transfer(s3_client)
        transfer.upload_file(
            DATABASE_PATH,
            self._bucket_name,
            key,
            callback=ProgressPercentage(
                key,
                float(getsize(DATABASE_PATH))
            ),
            extra_args={
                'StorageClass': 'REDUCED_REDUNDANCY',
            }
        )

    @staticmethod
    @echo
    def get_observation_name(tarred_measurement_set):
        (observation_name, _) = splitext(basename(tarred_measurement_set))
        return observation_name

    @echo
    def write_row(self,
                  observation_id,
                  scan_number,
                  begin_time,
                  end_time,
                  begin_hour_angle,
                  end_hour_angle,
                  spectral_window,
                  channel,
                  frequency,
                  max,
                  mean,
                  medabsdevmed,
                  median,
                  min,
                  npts,
                  quartile,
                  rms,
                  stddev,
                  sum,
                  sumsq,
                  var):
        self._connection.execute(
            self._insert_scan,
            observation_id=observation_id,
            scan_number=scan_number,
            begin_time=begin_time,
            end_time=end_time,
            begin_hour_angle=begin_hour_angle,
            end_hour_angle=end_hour_angle,
            spectral_window=spectral_window,
            channel=channel,
            frequency=frequency,
            max=max,
            mean=mean,
            medabsdevmed=medabsdevmed,
            median=median,
            min=min,
            npts=npts,
            quartile=quartile,
            rms=rms,
            stddev=stddev,
            sum=sum,
            sumsq=sumsq,
            var=var,
        )

    @echo
    def get_observation_id(self, observation_name):
        observation_id = self._map_observations.get(observation_name)
        if observation_id is None:
            observation_id = len(self._map_observations) + 1
            self._map_observations[observation_name] = observation_id
            self._connection.execute(
                self._insert_observation,
                observation_id=observation_id,
                description=observation_name,
            )
        LOG.info('Obs: {0} - {1}'.format(observation_name, observation_id))
        return observation_id


def parse_args():
    """
    This is called via Casa so we have to be a bit careful
    :return:
    """
    parser = argparse.ArgumentParser('Get the arguments')
    parser.add_argument('bucket_name', help='the bucket name')
    parser.add_argument('folder_name', help='the folder in the bucket with the data')
    # parser.add_argument('arguments', nargs='*', help='the arguments')

    parser.add_argument('--nologger', action="store_true")
    parser.add_argument('--log2term', action="store_true")
    parser.add_argument('--logfile')
    parser.add_argument('-c', '--call')

    return parser.parse_args()


if __name__ == "__main__" and __package__ is None:
    args = parse_args()
    LOG.info(args)

    generate_statistics = GenerateStatistics(args.bucket_name, args.folder_name)
    try:
        generate_statistics.run()
    except GenerateStatisticsException:
        LOG.exception('GenerateStatisticsException')
