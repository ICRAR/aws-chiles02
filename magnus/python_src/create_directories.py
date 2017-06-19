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
Move the data from S3
"""
import argparse
import logging
from os import makedirs
from os.path import exists, join

LOG = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('key_list', help='the key list file')
    parser.add_argument('destination', help='the root destination')

    return parser.parse_args()


def create_directories(key_list, destination):
    with open(key_list, "r") as input_file:
        for key in input_file.read():
            elements = key.split('/')
            full_destination = join(destination, elements[0], elements[1])
            if not exists(full_destination):
                LOG.info('Creating {0}'.format(full_destination))
                makedirs(full_destination)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parse_args()

    LOG.info(args)
    create_directories(args.key_list, args.destination)
