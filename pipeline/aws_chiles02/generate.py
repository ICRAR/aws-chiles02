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
Provide a single interface into building the images
"""
import argparse
import logging
import os
import sys

from configobj import ConfigObj


class Generate(object):
    def __init__(self):
        self._arguments = None

        if len(sys.argv) == 0:
            # Build the command string
            self._build_interactive_command_line()
        else:
            parser = argparse.ArgumentParser(
                description='Start the various generate scripts',
                usage='''generate [<args>]

        The most commonly used generate commands are:
           --clean       Record changes to the repository
           --jpeg2000    Produce the files required for JPEG2000
           --mstransform Split the input data into small chunks
           --uvsub       Perform the sky model subtraction

        The normal order is:
           mstransform
           uvsub
           clean
           jpeg2000
        ''')
            group = parser.add_mutually_exclusive_group()
            group.add_argument('--create', action='store_true', help='Create an EC2 cluster')
            group.add_argument('--use', action='store_true', help='Use an EC2 cluster')
            group.add_argument('--json', action='store_true', help='Print the JSOn for the graph')

            parser.add_argument('--clean', action='store_true', help='Perform a clean')
            parser.add_argument('--jpeg2000', action='store_true', help='Perform the production of the jpeg2000')
            parser.add_argument('--mstransform', action='store_true', help='Perform the mstransform')
            parser.add_argument('--uvsub', action='store_true', help='Perform the uvsub')

            parser.add_argument('--bucket', action='store', help='The S3 bucket to use', default='13b-266')
            parser.add_argument('--input_dir', action='store', help='The S3 folder to start from')
            parser.add_argument('--output_dir', action='store', help='The S3 folder to store the results in')
            parser.add_argument('--volume', action='store', help='The directory on the host to bind to the Docker Apps')
            parser.add_argument('--arcsec', action='store', help='The number of arcsec', default='2')
            parser.add_argument('--only_image', action='store_true', help='Store only the image to S3', default=True)
            parser.add_argument('--frequency_width', type=int, help='The frequency width for the split', default=4)
            parser.add_argument('--shutdown', action='store_true', help='Add a shutdown drop', default=True)
            parser.add_argument('--iterations', type=int, help='The number of clean iterations', default=10)
            parser.add_argument('--w_projection_planes', type=int, help='The number of w projections planes', default=24)
            parser.add_argument('--robust', type=float, help='The robust value for clean', default=0.8)
            parser.add_argument('--image_size', type=int, help='The image size for clean', default=2048)

            parser.add_argument('--ami', action='store', help='The AMI to use')
            parser.add_argument('--spot_price', type=float, action='store', help='The spot price we are prepared to pay')
            parser.add_argument('--min_frequency', type=int, action='store', help='The minimum frequency for this run')
            parser.add_argument('--max_frequency', type=int, action='store', help='The maximum frequency for this run')
            parser.add_argument('--nodes', type=int, action='store', help='The number of nodes to start')
            parser.add_argument('--dim_ip_address', action='store', help='The DIM IP address')

            parser.add_argument('-v', '--verbosity', action='count', help='Increase output verbosity', default=0)

            # Now build the argument namespace
            self._arguments = parser.parse_args()

    def _build_interactive_command_line(self):
        path_dirname, filename = os.path.split(__file__)
        config_file_name = '{0}/aws-chiles02.settings'.format(path_dirname)
        if os.path.exists(config_file_name):
            config = ConfigObj(config_file_name)
        else:
            config = ConfigObj()
            config.filename = config_file_name

        self._arguments = argparse.Namespace()

        # Write the arguments
        config.write()


def main():
    logging.basicConfig(level=logging.INFO)
    Generate()

if __name__ == '__main__':
    main()
