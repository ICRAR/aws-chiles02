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

from casapy_code.casa_common import parse_args
from casapy_code.echo import echo

casalog.filter('DEBUGGING')
LOG = logging.getLogger(__name__)


@echo
def do_concatenate(out_filename, input_files):
    """
    Perform the CONCATENATION step
    :param input_files:
    :param out_filename:
    """

    try:
        # virtualconcat doesn't need an import - it is just available in casapy
        final = virtualconcat(vis=input_files, concatvis=out_filename)
        final.done()

        exportfits(imagename=out_filename, fitsimage='{0}.fits'.format(out_filename))
    except Exception:
        LOG.exception('*********\nConcatenate exception: \n***********')

args = parse_args()
LOG.info(args)

# ignore the output directory
do_concatenate(
        args.arguments[1],
        args.arguments[2:])
