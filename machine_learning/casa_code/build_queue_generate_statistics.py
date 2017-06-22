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
Create a file based queue
"""
import argparse
import logging
import sys
from os.path import abspath, join, split

from configobj import ConfigObj
from sqlalchemy import create_engine, select

from casa_code.database import TASK

LOG = logging.getLogger(__name__)


def run(**keywords):
    database_login = 'mysql+pymysql://{0}:{1}@{2}/{3}'.format(
        keywords['database_user'],
        keywords['database_password'],
        keywords['database_hostname'],
        keywords['database_name']
    )
    engine = create_engine(database_login)
    connection = engine.connect()

    with open(keywords['queue_file_name'], "w") as queue:
        for row in connection.execute(select([TASK]).where(TASK.c.status == 0).order_by(TASK.c.task_id)):
            queue.write(
                '{0}\n'.format(row[TASK.c.task_id])
            )


def parse_args():
    path_dirname, _ = split(abspath(__file__))
    settings_file_name = join(path_dirname, 'scan.settings')
    parser = argparse.ArgumentParser()
    parser.add_argument('--settings_file_name', help='The settings file', default=settings_file_name)
    parser.add_argument('queue_file_name', help='The settings file', default=settings_file_name)

    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    LOG.info(args)
    LOG.info('PYTHONPATH = {0}'.format(sys.path))

    keyword_dictionary = vars(args)
    keyword_dictionary.update(ConfigObj(args.settings_file_name))

    LOG.debug('args: {0}'.format(keyword_dictionary))

    run(**keyword_dictionary)
