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
def do_clean(cube_dir, min_freq, max_freq, iterations, arcsec, w_projection_planes, robust, image_size, clean_channel_average, in_dirs):
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
              mode='frequency' if clean_channel_average == '' else 'channel',
              restfreq='1420.405752MHz',
              nchan=-1,
              start=0,
              width=clean_channel_average,
              interpolation='nearest',
              gridmode='widefield',
              niter=iterations,
              gain=0.1,
              threshold='0.0mJy',
              imsize=[image_size],
              cell=[arcsec],
              wprojplanes=w_projection_planes,
              weighting='briggs',
              robust=robust,
              usescratch=False) # Don't overwrite the model data col
    except Exception:
        LOG.exception('*********\nClean exception: \n***********')

    # IA used to report the statistics to the log file
    ia.open(outfile+'.image')
    ia.statistics(verbose=True,axes=[0,1])
    # IA used to make squashed images.
    ia.moments(moments=[-1],outfile=outfile+'image.mom.mean_freq')
    ia.moments(moments=[-1],axis=0,outfile=outfile+'image.mom.mean_ra')
    # IA used to make slices.
    smry=ia.summary()
    xpos=2967/4096*smry()['shape'][0]
    ypos=4095/4096*smry()['shape'][1]
    slice=ia.getslice(x=[xpos,xpos],y=[0,ypos])
    for n in range(0,len(slice)):
        print slice['ypos'][n],slice['pixel'][n]

    ## How do I print inside AWS ????
    ## pl.plot(slice['ypos'],slice['pixel'])
    ia.close()

    exportfits(imagename='{0}.image'.format(outfile), fitsimage='{0}.fits'.format(outfile))

args = parse_args()
LOG.info(args)

do_clean(
    cube_dir=args.arguments[0],
    min_freq=int(args.arguments[1]),
    max_freq=int(args.arguments[2]),
    iterations=int(args.arguments[3]),
    arcsec=args.arguments[4],
    w_projection_planes=int(args.arguments[5]),
    robust=float(args.arguments[6]),
    image_size=int(args.arguments[7]),
    clean_channel_average=args.arguments[8] if args.arguments[8] == '' else int(args.arguments[8]),
    in_dirs=args.arguments[9:])
