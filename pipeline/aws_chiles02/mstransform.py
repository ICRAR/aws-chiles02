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
import shutil

from aws_chiles02.freq_map import freq_map
from aws_chiles02.casa_common import find_file, parse_args
from aws_chiles02.echo import echo
from mstransform import mstransform

casalog.filter('DEBUGGING')
DEBUG = False
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)


@echo
def do_mstransform(infile, outdir, min_freq, max_freq, bottom_edge, width_freq=15.625):
    """
    Perform the MS_TRANSFORM step

    :param infile:
    :param outdir:
    :param min_freq:
    :param max_freq:
    :param bottom_edge
    :param width_freq:
    :return:
    """
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    spw_range = freq_map(min_freq, max_freq, bottom_edge)
    step_freq = max_freq - min_freq
    no_chan = int(step_freq * 1000.0 / width_freq)  # MHz/kHz!!

    outfile = os.path.join(outdir, 'vis_{0}~{1}'.format(min_freq, max_freq))
    LOG.info('working on: {0}'.format(outfile))
    if not DEBUG:
        if os.path.exists(outfile):
            shutil.rmtree(outfile)
        try:
            # dump_all()
            mstransform(
                    vis=infile,
                    outputvis=outfile,
                    regridms=True,
                    restfreq='1420.405752MHz',
                    mode='frequency',
                    nchan=no_chan,
                    outframe='lsrk',
                    interpolation='linear',
                    veltype='radio',
                    start='{0}MHz'.format(min_freq),
                    width='{0}kHz'.format(width_freq),
                    spw=spw_range,
                    combinespws=True,
                    nspw=1,
                    createmms=False,
                    datacolumn="data")

        except Exception:
            LOG.exception('*********\nmstransform exception:\n***********')
    else:
        LOG.info('''
mstransform(vis={0},
outputvis={1},
start={2}MHz,
width={3},
spw={4},
nchan={5})'''.format(infile, outfile, min_freq, width_freq, spw_range, no_chan))


args = parse_args()
LOG.info(args)

do_mstransform(
        find_file(args.arguments[0]),
        args.arguments[1],
        int(args.arguments[2]),
        int(args.arguments[3]),
        float(args.arguments[4]))
