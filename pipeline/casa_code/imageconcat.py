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
Perform the MS Transform
"""
import logging
import os

from casa_code.casa_common import parse_args
from casa_code.echo import echo

casalog.filter('DEBUGGING')
logging.info('Starting logger for...')
LOG = logging.getLogger('imageconcat')


@echo
def do_imageconcat(cube_dir, out_filename, input_files):
    """
    Perform the CONCATENATION step
    :param input_files:
    :param out_filename:
    """
    if not os.path.exists(cube_dir):
        os.makedirs(cube_dir)

    outfile = os.path.join(cube_dir, out_filename)
    LOG.info('imageconcat(vis={0}, imagename={1})'.format(str(input_files), outfile))

    try:
        # IA used to report the statistics to the log file
        # for input_file in input_files:
        #     ia.open(input_file)
        #     ia.statistics(verbose=True,axes=[0,1])
        #     ia.close()

        # ia doesn't need an import - it is just available in casa
        final = ia.imageconcat(infiles=input_files, outfile=outfile, relax=True, overwrite=True)
        final.done()

        # ia.open(out_filename)
        # ia.statistics(verbose=True,axes=[0,1])
        # ia.close()

        exportfits(imagename=outfile, fitsimage='{0}.fits'.format(outfile))
    except Exception:
        LOG.exception('*********\nConcatenate exception: \n***********')

if __name__ == "__main__":
    args = parse_args()
    LOG.info(args)

    do_imageconcat(
        args.arguments[0],
        args.arguments[1],
        args.arguments[2:])
