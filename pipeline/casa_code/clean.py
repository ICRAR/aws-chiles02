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
Perform the Clean
"""
import logging
import os

from casa_code.casa_common import parse_args
from casa_code.echo import echo

casalog.filter('DEBUGGING')
logging.info('Starting logger for...')
LOG = logging.getLogger('clean')


@echo
def do_clean(cube_dir, min_freq, max_freq, iterations, arcsecs, in_dirs):
    """
    Perform the CLEAN step

    """
    if not os.path.exists(cube_dir):
        os.makedirs(cube_dir)

    outfile = os.path.join(cube_dir, 'clean_{0}~{1}'.format(min_freq, max_freq))
    LOG.info('clean(vis={0}, imagename={1})'.format(str(in_dirs), outfile))
    try:
        # dump_all()
        clean(vis=in_dirs,
              imagename=outfile,
              field='deepfield',
              spw='',
              mode='frequency',
              restfreq='1420.405752MHz',
              nchan=-1,
              start='',
              width='',
              interpolation='nearest',
              niter=iterations,
              gain=0.1,
              threshold='0.0mJy',
              imsize=[2048],
              cell=[arcsecs],
              weighting='natural',
              usescratch=False) # Don't overwrite the model data col
    except Exception:
        LOG.exception('*********\nClean exception: \n***********')

    exportfits(imagename='{0}.image'.format(outfile), fitsimage='{0}.fits'.format(outfile))

args = parse_args()
LOG.info(args)

do_clean(
    args.arguments[0],
    int(args.arguments[1]),
    int(args.arguments[2]),
    int(args.arguments[3]),
    args.arguments[4],
    args.arguments[5:])
