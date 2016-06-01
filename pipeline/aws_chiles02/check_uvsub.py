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

from aws_chiles02.generate_uvsub_graph import WorkToDo, get_s3_uvsub_name, get_s3_split_name

LOG = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser('Check what data has been removed the sky model from')
    parser.add_argument('bucket', help='the bucket to access')
    parser.add_argument('-w', '--width', type=int, help='the frequency width', default=4)
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)
    arguments = parse_arguments()

    work_to_do = WorkToDo(arguments.width, arguments.bucket, get_s3_uvsub_name(arguments.width), get_s3_split_name(arguments.width))
    work_to_do.calculate_work_to_do()

    for work_item in work_to_do.work_to_do:
        LOG.info(work_item)

if __name__ == "__main__":
    main()
