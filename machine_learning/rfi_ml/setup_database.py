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
import logging
import sys
from os.path import abspath, basename, exists, join, split, splitext

import boto3
from configobj import ConfigObj
from sqlalchemy import create_engine

from rfi_ml.database import METADATA, OBSERVATION, TASK, TASK_NOT_PROCESSED

LOG = logging.getLogger(__name__)


def check_keys_exist(*positional_args, **keywords):
    for arg in positional_args:
        if keywords.get(arg) is None:
            raise RuntimeError('Missing the keyword {0}'.format(arg))


def setup_database(**keywords):
    check_keys_exist(
        'database_user',
        'database_password',
        'database_hostname',
        'database_name',
        'bucket_name',
        'folder_name',
        **keywords
    )
    database_login = 'mysql+pymysql://{0}:{1}@{2}/{3}'.format(keywords['database_user'], keywords['database_password'], keywords['database_hostname'], keywords['database_name'])
    engine = create_engine(database_login)

    # Create the database
    METADATA.create_all(engine)
    connection = engine.connect()

    map_observations = {}
    insert_task = TASK.insert()
    insert_observation = OBSERVATION.insert()

    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)
    bucket = s3.Bucket(keywords['bucket_name'])
    for key in bucket.objects.filter(Prefix='{0}'.format(keywords['folder_name'])):
        (observation_name, _) = splitext(basename(key.key))

        observation_id = map_observations.get(observation_name)
        if observation_id is None:
            observation_id = len(map_observations) + 1
            map_observations[observation_name] = observation_id
            connection.execute(
                insert_observation,
                observation_id=observation_id,
                description=observation_name,
            )

        connection.execute(
            insert_task,
            observation_id=observation_id,
            s3_key=key.key,
            status=TASK_NOT_PROCESSED,
        )


def parse_args():
    """
    This is called via Casa so we have to be a bit careful
    :return:
    """
    path_dirname, _ = split(abspath(__file__))
    settings_file_name = join(path_dirname, 'scan.settings')

    parser = argparse.ArgumentParser()
    parser.add_argument('--lower_boto_logging', action='store_true', help='Reduce the amount of logging boto3 does', default=True)
    parser.add_argument('--display_python_path', action='store_true', help='Show the Python path', default=True)
    parser.add_argument('-v', '--verbose', action='count', help='The verbosity level', default=1)
    parser.add_argument('--settings_file', help='The settings file', default=settings_file_name)
    parser.add_argument('bucket_name', help='the bucket name')
    parser.add_argument('folder_name', help='the folder in the bucket with the data')

    if len(sys.argv[1:]) == 0:
        return parser.parse_args(['-h'])
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    LOG.info(args)

    # Check the settings file exists
    config_file_name = args.settings_file
    if not exists(args.settings_file):
        raise RuntimeError('No configuration file {0}'.format(config_file_name))

    # Configure the logging levels
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(len(levels) - 1, args.verbose)]  # capped to number of levels
    logging.basicConfig(level=level, format='%(asctime)-15s:' + logging.BASIC_FORMAT)

    if args.lower_boto_logging:
        logging.getLogger('boto3').setLevel(logging.WARNING)
        logging.getLogger('botocore').setLevel(logging.WARNING)
        logging.getLogger('nose').setLevel(logging.WARNING)
        logging.getLogger('s3transfer').setLevel(logging.WARNING)

    if args.display_python_path:
        LOG.info('PYTHONPATH = {0}'.format(sys.path))

    keyword_dictionary = vars(args)
    keyword_dictionary.update(ConfigObj(args.settings_file))

    LOG.debug('args: {0}'.format(keyword_dictionary))
    setup_database(**keyword_dictionary)
    LOG.info('All done.')
