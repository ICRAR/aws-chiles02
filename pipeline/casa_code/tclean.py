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
def do_tclean(cube_dir, min_freq, max_freq, iterations, arcsec, w_projection_planes, clean_weighting_uv, robust, image_size, clean_channel_average, region_file, produce_qa, build_fits, in_dirs):
    """
    Perform the CLEAN step

    """
    if not os.path.exists(cube_dir):
        os.makedirs(cube_dir)

    delete_wtsp=True ## Temp hack till weight spectrum makes sense

    outfile = os.path.join(cube_dir, 'clean_{0}~{1}'.format(min_freq, max_freq))
    # If region_file is given check it exisits. If not try some paths.
    # If still not set to blank to prevent failure
    if (region_file):
        if (os.path.isfile(region_file)==False):
            LOG.info('Did not find file: {0}'.format(region_file))
            region_file='/home/ec2-user/aws-chiles02/LSM/'+region_file
            LOG.info('Setting file to default path: {0}'.format(region_file))
        if (os.path.isfile(region_file)==False):
            LOG.info('Did not find file: {0}'.format(region_file))
            region_file=''
            LOG.info('Setting file to blank: {0}'.format(region_file))

    LOG.info('tclean(vis={0}, imagename={1})'.format(str(in_dirs), outfile))
    #if isinstance(in_dirs,list):
    #    cvis=in_dirs[0].replace('.ms','.concat.ms')
    #    concat(vis=in_dirs,concatvis=cvis)
    #    in_dirs=cvis
    # else single ms file
    try:
        # dump_all()
        if delete_wtsp: # Temp hack till weight spectrum makes sense
          if isinstance(in_dirs,list):
            for vis_file in in_dirs:
                print('Removing weight spectrum from %s'%(vis_file))
                tb.open(vis_file,nomodify=False)
                if 'WEIGHT_SPECTRUM' in tb.colnames():
                    tb.removecols('WEIGHT_SPECTRUM')
                    tb.removecols('SIGMA_SPECTRUM')
                tb.close()
          else:
            # else single ms file
            print('Removing weight spectrum from %s'%(in_dirs))
            tb.open(in_dirs,nomodify=False)
            if 'WEIGHT_SPECTRUM' in tb.colnames():
                tb.removecols('WEIGHT_SPECTRUM')
                tb.removecols('SIGMA_SPECTRUM')
            tb.close()
        if clean_weighting_uv == 'briggs':
            tclean(vis=in_dirs,
                   field='deepfield',
                   spw='*',
                   datacolumn='data',
                   imagename=outfile,
                   imsize=[image_size],
                   cell=[arcsec],
                   specmode='cube',
                   chanchunks=-1,
                   nchan=-1,
                   start=0,
                   width=clean_channel_average,
                   mask=region_file,
                   outframe='BARY',
                   restfreq='1420.405752MHz',
                   interpolation='linear',
                   gridder='widefield',
                   wprojplanes=w_projection_planes,
                   pblimit=-0.2,
                   deconvolver='clark',
                   weighting='briggs',
                   robust=robust,
                   niter=iterations,
                   gain=0.1,
                   threshold='0.0mJy',
                   savemodel='virtual')  # Don't overwrite the model data col
        else:
            tclean(vis=in_dirs,
                   field='deepfield',
                   spw='*',
                   datacolumn='data',
                   imagename=outfile,
                   imsize=[image_size],
                   cell=[arcsec],
                   specmode='cube',
                   chanchunks=-1,
                   nchan=-1,
                   start=0,
                   width=clean_channel_average,
                   mask=region_file,
                   outframe='BARY',
                   restfreq='1420.405752MHz',
                   interpolation='linear',
                   gridder='widefield',
                   wprojplanes=w_projection_planes,
                   pblimit=-0.2,
                   deconvolver='clark',
                   weighting=clean_weighting_uv,
                   robust=robust,
                   niter=iterations,
                   gain=0.1,
                   threshold='0.0mJy',
                   savemodel='virtual')  # Don't overwrite the model data col
    except Exception:
        LOG.exception('*********\nClean exception: \n***********')

    # Make a smaller verision of the image cube
    if image_size > 2048:
        ia.open(outfile+'.image')
        # box=rg.box([image_size/4,image_size*3/4],[image_size/4,image_size*3/4])
        # box=rg.box([1024,1024],[3072,3072])
        box = rg.box([image_size/2-1024,image_size/2-1024],[image_size/2+1024,image_size/2+1024])
        im2 = ia.subimage(outfile+'.image.centre', box, overwrite=True)
        im2.done()
        ia.close()

    # Make a smaller version of the PDF cube
    ia.open(outfile+'.psf')
    box = rg.box([image_size/2-128, image_size/2-128], [image_size/2+128, image_size/2+128])
    im2 = ia.subimage(outfile+'.psf.centre', box, overwrite=True)
    im2.done()
    ia.close()

    if produce_qa == 'yes':
        import numpy as np
        import matplotlib.pyplot as pl
        # IA used to report the statistics to the log file
        ia.open(outfile+'.image')
        ia.statistics(verbose=True,axes=[0,1])
        # IA used to make squashed images.
        #ia.moments(moments=[-1], outfile=outfile+'.image.mom.mean_freq')
        #ia.moments(moments=[-1], axis=0, outfile=outfile+'.image.mom.mean_ra')
        c=ia.collapse(function='mean', axes=3,outfile=outfile+'.image.mom.mean_freq',overwrite=True)
        c.done()
        c=ia.collapse(function='mean', axes=0, outfile=outfile+'.image.mom.mean_ra',overwrite=True)
        c.done()
        # IA used to make slices.
        smry = ia.summary()
        xpos = 2967.0 / 4096 * smry['shape'][0]
        ypos = 4095.0 / 4096 * smry['shape'][1]
        box = rg.box([xpos - 2, 0], [xpos + 2, ypos])
        c=ia.moments(moments=[-1], axis=0, region=box, outfile=outfile+'image.mom.slice_ra',overwrite=True)
        c.done()
        #
        # We will get rid of this if the slice above works
        slce = []
        for m in range(0, smry['shape'][3]):
            slce.append(ia.getslice(x=[xpos, xpos], y=[0, ypos], coord=[0, 0, 0, m]))
        #
        # Print out text version
        fo = open(outfile + '.image.slice.txt', 'w')
        for n in range(0, len(slce[0]['ypos'])):
            line = [slce[0]['ypos'][n]]
            for m in range(0, len(slce)):
                line.append(slce[m]['pixel'][n])
            print>> fo, line
        fo.close()
        for m in range(0, len(slce)):
            pl.plot(slce[m]['ypos'], slce[m]['pixel'] * 1e3)
        pl.xlabel('Declination (pixels)')
        pl.ylabel('Amplitude (mJy)')
        pl.title('Slice along sidelobe for ' + outfile)
        pl.savefig(outfile + '.image.slice.svg')
        pl.clf()
        #
        # IA used to make profiles.
        # Source near centre profile
        xpos = 1992.0 / 4096 * smry['shape'][0]
        ypos = 2218.0 / 4096 * smry['shape'][1]
        box = rg.box([xpos - 2, ypos - 2], [xpos + 2, ypos + 2])
        slce = ia.getprofile(region=box, unit='MHz', function='mean', axis=3)
        fo = open(outfile + '.image.onsource_centre.txt', 'w')
        for n in range(0, len(slce['coords'])):
            print>> fo, slce['coords'][n], slce['values'][n]
        fo.close()
        pl.plot(slce['coords'], slce['values'] * 1e3)
        pl.xlabel('Frequency (MHz)')
        pl.ylabel('Amplitude (mJy)')
        pl.title('Slice central source ' + outfile)
        pl.savefig(outfile + '.image.onsource_centre.svg')
        pl.clf()
        # Source near edge profile
        xpos = 2972.0 / 4096 * smry['shape'][0]
        ypos = 155.0 / 4096 * smry['shape'][1]
        box = rg.box([xpos - 2, ypos - 2], [xpos + 2, ypos + 2])
        slce = ia.getprofile(region=box, unit='MHz', function='mean', axis=3)
        fo = open(outfile + '.image.onsource_south.txt', 'w')
        for n in range(0, len(slce['coords'])):
            print>> fo, slce['coords'][n], slce['values'][n]
        fo.close()
        pl.plot(slce['coords'], slce['values'] * 1e3)
        pl.xlabel('Frequency (MHz)')
        pl.ylabel('Amplitude (mJy)')
        pl.title('Slice central source ' + outfile)
        pl.savefig(outfile + '.image.onsource_south.svg')
        pl.clf()
        # Boresight profile
        box = rg.box([image_size / 2 - 2, image_size / 2 - 2], [image_size / 2 + 2, image_size / 2 + 2])
        slce = ia.getprofile(region=box, unit='MHz', function='mean', axis=3)
        fo = open(outfile + '.image.boresight.txt', 'w')
        for n in range(0, len(slce['coords'])):
            print>> fo, slce['coords'][n], slce['values'][n]
        fo.close()
        pl.plot(slce['coords'], slce['values'] * 1e3)
        pl.xlabel('Frequency (MHz)')
        pl.ylabel('Amplitude (mJy)')
        pl.title('Slice central source ' + outfile)
        pl.savefig(outfile+'.image.boresight.svg')
        pl.clf()
        # RMS Stats
        sts=ia.statistics(axes=[0,1],verbose=False)
        fo = open(outfile+'.image.rms.txt', 'w')
        for n in range(0, len(sts['rms'])):
            print>> fo, slce['coords'][n], sts['rms'][n]
        fo.close()
        pl.plot(slce['coords'], sts['rms'] * 1e3)
        pl.xlabel('Frequency (MHz)')
        pl.ylabel('RMS (mJy)')
        pl.title('RMS for ' + outfile)
        pl.savefig(outfile+'.image.rms.svg')
        pl.clf()
        # Histograms
        sts=ia.histograms()
        fo = open(outfile+'.image.histo.txt', 'w')
        for n in range(0, len(sts['values'])):
            print>> fo, sts['values'][n], sts['counts'][n]
        fo.close()
        pl.plot(sts['values'] * 1e3, np.log10(sts['counts']))
        pl.xlabel('Bin (mJy)')
        pl.ylabel('log10 of No. of Values')
        pl.title('Histogram for ' + outfile)
        pl.savefig(outfile+'.image.histo.svg')
        pl.clf()
        # Beams --- sometimes this information is missing, so only generate if what is needed is there
        sts=ia.summary()
        if 'perplanebeams' in sts.keys():
            sts=sts['perplanebeams']
            bmj = []
            bmn = []
            bmp = []
            fo = open(outfile+'.image.beam.txt', 'w')
            for n in range(0, sts['nChannels']):
                print>> fo, slce['coords'][n], sts['beams']['*'+str(n)]['*0']
            fo.close()
            for n in range(0, sts['nChannels']):
                bmj.append(sts['beams']['*'+str(n)]['*0']['major']['value'])
                bmn.append(sts['beams']['*'+str(n)]['*0']['minor']['value'])
                bmp.append(sts['beams']['*'+str(n)]['*0']['positionangle']['value']/57.3)
            pl.plot(slce['coords'], bmj)
            pl.plot(slce['coords'], bmn)
            pl.plot(slce['coords'], bmp)
            pl.xlabel('Frequency (MHz)')
            pl.ylabel('Beam Axes (major, minor & PA (rad)')
            pl.title('Beam Parameters for ' + outfile)
            pl.savefig(outfile+'.image.beam.svg')
            pl.clf()
        ia.close()

    if build_fits == 'yes':
        exportfits(imagename='{0}.image'.format(outfile), fitsimage='{0}.fits'.format(outfile))


if __name__ == "__main__":
    args = parse_args()
    LOG.info(args)

    do_tclean(
        cube_dir=args.arguments[0],
        min_freq=int(args.arguments[1]),
        max_freq=int(args.arguments[2]),
        iterations=int(args.arguments[3]),
        arcsec=args.arguments[4],
        w_projection_planes=int(args.arguments[5]),
        clean_weighting_uv=args.arguments[6],
        robust=float(args.arguments[7]),
        image_size=int(args.arguments[8]),
        clean_channel_average=args.arguments[9] if args.arguments[9] == 'None' else int(args.arguments[9]),
        region_file=args.arguments[10] if args.arguments[10] != 'None' else '',
        produce_qa=args.arguments[11],
        build_fits=args.arguments[12],
        in_dirs=args.arguments[13:])
