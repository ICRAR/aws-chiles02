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

from configobj import ConfigObj

from aws_chiles02.common import set_logging_level, get_input_mode
from aws_chiles02.generate_uvsub_graph import WorkToDo, get_s3_split_name
from aws_chiles02.get_argument import GetArguments

LOG = logging.getLogger(__name__)


def get_arguments():
    path_dirname, filename = split(__file__)
    config_file_name = '{0}/aws-chiles02.settings'.format(path_dirname)
    if exists(config_file_name):
        config = ConfigObj(config_file_name)
    else:
        config = ConfigObj()
        config.filename = config_file_name

    mode = get_input_mode()
    args = GetArguments(config=config, mode=mode)
    args.get('bucket_name', 'Bucket name', help_text='the bucket to access', default='13b-266')
    args.get('width', 'Frequency width', data_type=int, help_text='the frequency width', default=4)
    args.get('uvsub_directory_name', 'The directory name for the uvsub output', help_text='the directory name for the uvsub output')
    args.get('frequency_range', 'Do you want to specify a range of frequencies', help_text='Do you want to specify a range of frequencies comma separated', default='')

    # Write the arguments
    config.write()

    return config


def main():
    keywords = get_arguments()

    work_to_do = WorkToDo(
        width=keywords['width'],
        bucket_name=keywords['bucket_name'],
        s3_uvsub_name=keywords['uvsub_directory_name'],
        s3_split_name=get_s3_split_name(keywords['width']),
        frequency_range=keywords['frequency_range'],
    )
    work_to_do.calculate_work_to_do()

    LOG.info("These are items still needing to be processed.")
    for work_item in work_to_do.work_to_do:
        LOG.info(work_item)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
