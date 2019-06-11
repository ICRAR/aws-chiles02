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
Check the uvsubs
"""
import argparse
import logging
from os.path import exists, split

from aws_chiles02.common import get_config
from aws_chiles02.generate_uvsub_graph import WorkToDo

LOGGER = logging.getLogger(__name__)


def main(command_line_):
    if command_line_.config_file is not None:
        if exists(command_line_.config_file):
            yaml_filename = command_line_
        else:
            LOGGER.error('Invalid configuration filename: {}'.format(command_line_.config_file))
            return
    else:
        path_dirname, filename = split(__file__)
        yaml_filename = '{0}/aws-chiles02.yaml'.format(path_dirname)

    LOGGER.info('Reading YAML file {}'.format(yaml_filename))
    config = get_config(yaml_filename, 'check_uvsub')

    work_to_do = WorkToDo(
        width=config['width'],
        bucket_name=config['bucket_name'],
        s3_uvsub_name=config['uvsub_directory_name'],
        s3_split_name=config['split_directory'],
        frequency_range=config['frequency_range'],
    )
    work_to_do.calculate_work_to_do()

    LOGGER.info("These {} items still needing to be processed.".format(len(work_to_do.work_to_do)))
    for work_item in work_to_do.work_to_do:
        LOGGER.info(work_item)
    LOGGER.info("There are {} items still needing to be processed.".format(len(work_to_do.work_to_do)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Check Splits')
    parser.add_argument(
        '--config_file',
        default=None,
        help='the config file for this run'
    )
    command_line = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format='{asctime}:{levelname}:{name}:{message}',
        style='{',
    )
    main(command_line)
