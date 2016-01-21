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
import re
import shutil

from common import INPUT_MS_SUFFIX
from echo import echo
from freq_map import freq_map
from mstransform import mstransform

casalog.filter('DEBUGGING')
DEBUG = True
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)


@echo
def do_mstransform(infile, outdir, min_freq, max_freq, step_freq, width_freq, sel_freq, spec_window):
    """
    Perform the MS_TRANSFORM step

    :param infile:
    :param outdir:
    :param min_freq:
    :param max_freq:
    :param step_freq:
    :param width_freq:
    :param sel_freq
    :param spec_window:
    :return:
    Adapted from loop.split.py with changes
    (1) deal with (max_freq - min_freq) % step_freq > 0
    (2) some debug info

    If spec_window is blank ('') then freq_map is called to define the spw selection
    """
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    steps = (max_freq - min_freq) / step_freq
    rem = (max_freq - min_freq) % step_freq
    if rem:
        steps += 1
    freq1 = min_freq
    freq2 = min_freq + step_freq
    bottom_edge = re.search('_[0-9]{3}_', infile)
    if bottom_edge:
        bedge = bottom_edge.group(0)
        bedge = int(bedge[1:4])

    if not sel_freq:
        steps = 1

    for i in range(steps):
        if sel_freq:
            if rem and i == steps - 1:
                freq_range = '%d~%d' % (min_freq + i * step_freq, max_freq)
                cvel_freq_range = '%f~%f' % (min_freq - 2 + i * step_freq, max_freq + 2)
            else:
                freq_range = str(freq1) + '~' + str(freq2)
                cvel_freq_range = str(int(freq1-2)) + '~' + str(int(freq2+2))
            spw_range = spec_window + ':' + freq_range + 'MHz'
            ms_spw_range = spec_window + ':' + cvel_freq_range + 'MHz'
            LOG.info('spw_range: {0}, mst_spw_range: {1}'.format(spw_range, ms_spw_range))
            # spanning spectral windows and selecting freq fails
            # so use freq_map
            # THEREFORE ~10 lines above are IGNORED!!
            ms_spw_range = freq_map(freq1, freq2, bedge)
        else:
            freq_range = 'min~max'
            spw_range = spec_window

        # If no spw is given then calculate from the max and min range
        if spec_window == '':
            spw_range = freq_map(freq1, freq2)

        no_chan = int(step_freq * 1000.0 / width_freq)  # MHz/kHz!!

        outfile = outdir + 'vis_' + freq_range
        LOG.info('working on: {0}'.format(outfile))
        if not DEBUG:
            if os.path.exists(outfile):
                shutil.rmtree(outfile)
            print 'working on: ' + outfile
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
                        start=str(freq1) + 'MHz',
                        width=str(width_freq) + 'kHz',
                        spw=ms_spw_range,
                        combinespws=True,
                        nspw=1,
                        createmms=False,
                        datacolumn="data")

            except Exception:
                LOG.exception('*********\nSplit exception: %s\n***********')
        else:
            LOG.info('''
mstransform(vis=%s,
outputvis=%s,
start=%s,
width=%s,
spw=%s,
nchan=%d)'''.format(infile, outfile, str(freq1)+'MHz', width_freq, ms_spw_range, no_chan))

        freq1 += step_freq
        freq2 += step_freq


@echo
def find_file(top_dir):
    for file_name in os.listdir(top_dir):
        if file_name.endswith(INPUT_MS_SUFFIX):
            return file_name

    return None


@echo
def parse_args():
    parser = argparse.ArgumentParser('Get the arguments')
    parser.add_argument('arguments', nargs='+', help='the arguments')

    parser.add_argument('--nologger', action="store_true")
    parser.add_argument('--log2term', action="store_true")
    parser.add_argument('--logfile')
    parser.add_argument('-c', '--call', action="store_true")

    return parser.parse_args()


args = parse_args()
LOG.info(args)

list_arguments = args['arguments']
do_mstransform(
        find_file(list_arguments[0]),
        list_arguments[1],
        list_arguments[2],
        list_arguments[3],
        list_arguments[4],
        list_arguments[5],
        list_arguments[6],
        list_arguments[7])
