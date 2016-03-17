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
LOG = logging.getLogger(__name__)


def do_mstransform(infile, outdir, min_freq, max_freq, bottom_edge, predict_subtract=False, width_freq=15.625):
    """
    Perform the MS_TRANSFORM step

    :param infile:
    :param outdir:
    :param min_freq:
    :param max_freq:
    :param bottom_edge:
    :param predict_subtract:
    :param width_freq:
    :return:
    """
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    spw_range = freq_map(min_freq, max_freq, bottom_edge)
    LOG.info('spw_range: {0}'.format(spw_range))
    if spw_range != '-1~-1':
        step_freq = max_freq - min_freq
        no_chan = int(step_freq * 1000.0 / width_freq)  # MHz/kHz!!

        outfile = os.path.join(outdir, 'vis_{0}~{1}'.format(min_freq, max_freq))
        LOG.info('working on: {0}'.format(outfile))
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

            if predict_subtract:
                # Richard the predict and subtract code goes here
                pass

        except Exception:
            LOG.exception('*********\nmstransform exception:\n***********')
        if (predict_subtract): # Also perform Predict and Subtract step
            # Predict and fill MODEL_DATA
            mod_spw=max_freq+min_freq)/2.0
            spw_range=freq_map(mod_spw,mod_spw,bottom_edge)
            mod_spw=re.split('\D+',spw_range)
            mod_spw=mod_spw[0]
                        
            ft(vis=outfile,
               field="",
               spw="",
               model=['epoch1gt4k_si_spw_'+str(mod_spw)+'.model.tt0',
                      'epoch1gt4k_si_spw_'+str(mod_spw)+'.model.tt1'], #Model
               nterms             =  2,     # SI model
               reffreq            =  "",     
               complist           =  "",    # use model
               incremental        =  False, # Replace, not add
               usescratch         =  True,  # Save in MODEL_DATA
            )
            # Subtract and fill CORRECTED_DATA
            uvsub(vis=outfile,
                  reverse=False)
    else:
        LOG.info('Outside spectral window')

args = parse_args()
LOG.info(args)

do_mstransform(
        find_file(args.arguments[0]),
        args.arguments[1],
        int(args.arguments[2]),
        int(args.arguments[3]),
        float(args.arguments[4]),
        args.arguments[5] == 'True')
