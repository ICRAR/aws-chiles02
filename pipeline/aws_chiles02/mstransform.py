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

"""
import argparse
import logging
import os
import shutil

from common import INPUT_MS_SUFFIX
from echo import echo
from mstransform import mstransform

casalog.filter('DEBUGGING')
DEBUG = False
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)


@echo
def do_mstransform(infile, outdir, min_freq, max_freq, width_freq=15.625):
    """
    Perform the MS_TRANSFORM step

    :param infile:
    :param outdir:
    :param min_freq:
    :param max_freq:
    :param width_freq:
    :return:
    """
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    ms_spw_range = '{0}~{1}MHz'.format(min_freq, max_freq)
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
                    spw=ms_spw_range,
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
nchan={5})'''.format(infile, outfile, min_freq, width_freq, ms_spw_range, no_chan))


@echo
def find_file(top_dir):
    for file_name in os.listdir(top_dir):
        if file_name.endswith(INPUT_MS_SUFFIX):
            return os.path.join(top_dir, file_name)

    return None


@echo
def parse_args():
    parser = argparse.ArgumentParser('Get the arguments')
    parser.add_argument('arguments', nargs='+', help='the arguments')

    parser.add_argument('--nologger', action="store_true")
    parser.add_argument('--log2term', action="store_true")
    parser.add_argument('--logfile')
    parser.add_argument('-c', '--call')

    return parser.parse_args()


args = parse_args()
LOG.info(args)

do_mstransform(
        find_file(args.arguments[0]),
        args.arguments[1],
        int(args.arguments[2]),
        int(args.arguments[3]))
