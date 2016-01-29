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
Perform the listobs
"""
import logging
import os

from aws_chiles02.parse_listobs import ParseListobs
from aws_chiles02.echo import echo
from aws_chiles02.casa_common import find_file, parse_args
from listobs import listobs

casalog.filter('DEBUGGING')
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)


@echo
def do_listobs(infile, outfile):
    # make sure the directory for the file exists
    dir_path, tail = os.path.split(outfile)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    listobs(vis=infile, verbose=False, listfile='/tmp/listfile.txt')

    parse_listobs = ParseListobs('/tmp/listfile.txt')
    parse_listobs.parse()
    json_string = parse_listobs.get_json_string()
    with open(outfile, mode='w') as out_file:
        out_file.write(json_string)


args = parse_args()
LOG.info(args)

do_listobs(
        find_file(args.arguments[0]),
        args.arguments[1])
