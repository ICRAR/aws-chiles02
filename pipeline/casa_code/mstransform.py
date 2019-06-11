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
import json
import logging
import os
import shutil

from casa_code.casa_common import find_file, parse_args
from casa_code.freq_map import freq_map

casalog.filter('DEBUGGING')
logging.info('Starting logger for...')
LOG = logging.getLogger('mstransform')


def do_mstransform(infile, outdir, min_freq, max_freq, list_obs_json):
    """
    Perform the MS_TRANSFORM step
    """
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    with open(list_obs_json, mode='r') as json_file:
        json_data = json.load(json_file)

    spw_range, width_freq = freq_map(min_freq, max_freq, json_data['Spectral Windows']['Spectral Windows'])
    step_freq = max_freq - min_freq
    no_chan = int(step_freq * 1000.0 / width_freq)  # MHz/kHz!!

    # TODO: needed if we use outframe='lsrk' or 'bary'
    # replaces selections above
    ms.open(infile)
    msspecinfo=ms.getspectralwindowinfo()
    width_chan=int(width_freq*1000/msspecinfo['0']['ChanWidth'])
    ms.close()
    im.selectvis(vis=infile)
    selinfo=im.advisechansel(freqstart=min_freq*1e6, freqend=max_freq*1e6, freqstep=width_freq*1e3, freqframe='BARY')
    spw_range=''
    # Use no_chan from above. This is the no channels in not out
    #no_chan=0
    for n in range(len(selinfo['ms_0']['spw'])):
        spw_range=spw_range+str(selinfo['ms_0']['spw'][n])+':'
        spw_range=spw_range+str(selinfo['ms_0']['start'][n])+'~'
        spw_range=spw_range+str(selinfo['ms_0']['start'][n]+selinfo['ms_0']['nchan'][n])
        #no_chan += selinfo['ms_0']['nchan'][n]
        #spw_range=spw_range+str(selinfo['ms_0']['spw'][n])
        #no_chan += selinfo['ms_0']['nchan'][n]
        if ((n+1)<len(selinfo['ms_0']['spw'])):
            spw_range=spw_range+','
    im.close()
    # TODO: HACK

    LOG.info('spw_range: {}, no_chan: {}, width_freq: {}'.format(spw_range, no_chan, width_freq))
    if spw_range.startswith('-1') or spw_range.endswith('-1'):
        LOG.info('The spw_range is {0} which is outside the spectral window '.format(spw_range))
    else:
        outfile = os.path.join(outdir, 'vis_{0}~{1}'.format(min_freq, max_freq))
        LOG.info('working on: {}'.format(outfile))
        if os.path.exists(outfile):
            shutil.rmtree(outfile)
        try:
            # dump_all()
            mstransform(
                vis=infile,
                outputvis=outfile,
                regridms=True,
                restfreq='1420.405752MHz',
                mode='channel',
                # nchan=no_chan,## With specific spw_range nchan is all
                outframe='TOPO',
                interpolation='linear',
                veltype='radio',
                #start=selinfo['ms_0']['start'][0], ## With specific spw_range start=0
                #width='{}kHz'.format(width_freq),
                width=width_chan, ## different form with channels
                spw=spw_range,
                combinespws=False,
                nspw=0,
                createmms=False,
                datacolumn="data"
            )
            ms.open(outfile)
            LOG.info('Created File: %s %s %s'%(infile,str(ms.getspectralwindowinfo()),str(ms.getscansummary())))
            ms.close()

        except Exception:
            LOG.exception('*********\nmstransform exception:\n***********')


if __name__ == "__main__":
    args = parse_args()
    LOG.info(args)

    do_mstransform(
        infile=find_file(args.arguments[0]),
        outdir=args.arguments[1],
        min_freq=int(args.arguments[2]),
        max_freq=int(args.arguments[3]),
        list_obs_json=args.arguments[4],
    )
