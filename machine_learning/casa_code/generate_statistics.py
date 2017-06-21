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
import sys
from os import makedirs
from os.path import abspath, exists, getsize, join, split

import boto3
from configobj import ConfigObj
from s3transfer import S3Transfer
from sqlalchemy import create_engine, select

from casa_code.casa_logging import CasaLogger, echo
from casa_code.common import ProgressPercentage, run_command, stopwatch
from casa_code.database import SCAN, TASK, TASK_NOT_PROCESSED, TASK_PROCESSED
from casa_code.get_statistics import GetStatistics

LOG = CasaLogger(__name__)


class GenerateStatisticsException(Exception):
    pass


class GenerateStatistics(object):
    def __init__(self, **keywords):
        for arg in ['database_user',
                    'database_password',
                    'database_hostname',
                    'database_name',
                    'bucket_name',
                    'folder_name',
                    'task_id']:
            if keywords.get(arg) is None:
                raise RuntimeError('Missing the keyword {0}'.format(arg))

        self._bucket_name = keywords['bucket_name']
        self._folder_name = keywords['folder_name']
        self._task_id = keywords['task_id']
        self._magnus = keywords['magnus']
        self._database_login = 'mysql+pymysql://{0}:{1}@{2}/{3}'.format(
            keywords['database_user'],
            keywords['database_password'],
            keywords['database_hostname'],
            keywords['database_name']
        )
        self._connection = None
        self._s3 = None
        self._observation_id = None
        self._s3_key = None
        self._insert_scan = SCAN.insert()
        self._root_dir = '/group/pawsey0216/kvinsen/chiles_data' if self._magnus else '/mnt/ssd01/lscratch/kevin'

    def setup(self):
        if not exists(self._root_dir):
            try:
                makedirs(self._root_dir)
            except OSError as exception:
                if not exists(self._root_dir):
                    raise exception

        session = boto3.Session(profile_name='aws-chiles02')
        self._s3 = session.resource('s3', use_ssl=False)
        engine = create_engine(self._database_login)
        self._connection = engine.connect()

        row = self._connection.execute(select([TASK]).where(TASK.c.task_id == self._task_id)).fetchone()
        if row is not None:
            self._task_id = row[TASK.c.task_id]
            self._observation_id = row[TASK.c.observation_id]
            self._s3_key = row[TASK.c.s3_key]
            return row[TASK.c.status] == TASK_NOT_PROCESSED

        return False

    def run(self):
        if not self.setup():
            LOG.info("The task_id: {0} has been processed or doesn't exist".format(self._task_id))
            # We've done this one
            return

        if self._magnus:
            measurement_set = join(self._root_dir, self._s3_key)
            measurement_set = measurement_set[:-4]
            with stopwatch('Add to database'):
                self.add_to_database(measurement_set)
        else:
            with stopwatch('Copy from S3'):
                measurement_set, temp_directory = self.copy_from_s3(self._s3_key)
            with stopwatch('Add to database'):
                self.add_to_database(measurement_set)
            self.delete_processed_data(temp_directory)

    @echo
    def copy_from_s3(self, measurement_set):
        temp_directory = join(self._s3_key, 'run_{0}'.format(self._task_id))
        makedirs(temp_directory)
        tar_file = join(temp_directory, 'tar_file.tar')
        s3_object = self._s3.Object(self._bucket_name, measurement_set)
        s3_size = s3_object.content_length
        s3_client = self._s3.meta.client
        transfer = S3Transfer(s3_client)
        transfer.download_file(
            self._bucket_name,
            measurement_set,
            tar_file,
            callback=ProgressPercentage(
                measurement_set,
                s3_size
            )
        )
        if not exists(tar_file):
            message = 'The tar file {0} does not exist'.format(tar_file)
            raise GenerateStatisticsException(message)

        # Check the sizes match
        tar_size = getsize(tar_file)
        if s3_size != tar_size:
            message = 'The sizes for {0} differ S3: {1}, local FS: {2}'.format(tar_file, s3_size, tar_size)
            raise GenerateStatisticsException(message)

        # The tar file exists and is the same size
        bash = 'tar -xvf {0} -C {1}'.format(tar_file, temp_directory)
        return_code = run_command(bash)

        elements = measurement_set.split('/')
        elements = elements[1].split('_')
        measurement_set_path = join(temp_directory, 'uvsub_{0}~{1}'.format(elements[0], elements[1]))
        path_exists = exists(measurement_set_path)
        if return_code != 0 or not path_exists:
            message = 'tar return_code: {0}, exists: {1}-{2}'.format(return_code, measurement_set_path, path_exists)
            raise GenerateStatisticsException(message)

        return measurement_set_path, temp_directory

    @echo
    def add_to_database(self, measurement_set):
        transaction = self._connection.begin()
        get_statistics = GetStatistics(measurement_set)
        get_statistics.extract_statistics(self, self._observation_id)
        self._connection.execute(
            TASK.update().where(TASK.c.task_id == self._task_id).values(status=TASK_PROCESSED)
        )
        transaction.commit()

    @staticmethod
    def delete_processed_data(temp_directory):
        shutil.rmtree(temp_directory)

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


def parse_args():
    """
    This is called via Casa so we have to be a bit careful
    :return:
    """
    path_dirname, _ = split(abspath(__file__))
    settings_file_name = join(path_dirname, 'scan.settings')
    parser = argparse.ArgumentParser()
    parser.add_argument('--nologger', action='store_true')
    parser.add_argument('--log2term', action='store_true')
    parser.add_argument('--nogui', action='store_true')
    parser.add_argument('--logfile', nargs=1)
    parser.add_argument('--nologfile', action='store_true')
    parser.add_argument('-c', '--call')
    parser.add_argument('bucket_name', help='the bucket name')
    parser.add_argument('folder_name', help='the folder in the bucket with the data')
    parser.add_argument('task_id', type=int)
    parser.add_argument('--settings_file_name', help='The settings file', default=settings_file_name)
    parser.add_argument('--magnus', action='store_true', default=False)

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    LOG.info(args)

    # Check the settings file exists
    if not exists(args.settings_file_name):
        raise RuntimeError('No configuration file {0}'.format(args.settings_file_name))

    LOG.info('PYTHONPATH = {0}'.format(sys.path))

    keyword_dictionary = vars(args)
    keyword_dictionary.update(ConfigObj(args.settings_file_name))

    LOG.debug('args: {0}'.format(keyword_dictionary))
    generate_stats = GenerateStatistics(**keyword_dictionary)
    try:
        generate_stats.run()
    except GenerateStatisticsException:
        LOG.exception('GenerateStatisticsException')
    LOG.info('All done.')
