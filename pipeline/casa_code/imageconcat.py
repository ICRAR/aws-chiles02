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
Perform the Image Concatination and Image Subtraction
"""
import logging
import os

from casa_code.casa_common import parse_args
from casa_code.echo import echo

casalog.filter('DEBUGGING')
logging.info('Starting logger for...')
LOG = logging.getLogger('imageconcat')


@echo
def do_imageconcat(cube_dir, out_filename, fit_order, build_fits, input_files):
    """
    Perform the CONCATENATION step
    """
    if not os.path.exists(cube_dir):
        os.makedirs(cube_dir)

    outfile = os.path.join(cube_dir, out_filename)
    #FitOrder=1
    #if build_fits == 'yes':
    #    FitOrder=2
    LOG.info('imageconcat(vis={0}, imagename={1}),fitorder={2}'.format(str(input_files), outfile,fit_order))

    try:
        # IA used to report the statistics to the log file
        # for input_file in input_files:
        #     ia.open(input_file)
        #     ia.statistics(verbose=True,axes=[0,1])
        #     ia.close()

        # ia doesn't need an import - it is just available in casa
        final = ia.imageconcat(infiles=input_files, outfile=outfile, relax=True, overwrite=True)
        final.done()
        ia.open(outfile)
        # find the good (<4 sigma) channels
        sts=ia.statistics(axes=[0,1],verbose=False)
        ia.close()
        mdn_rms=np.median(sts['rms'])   
        I=np.where((sts['rms']<4.0*mdn_rms)&(sts['rms']>0))[0] ## Add a filter for blanked channels? where(rms<4*mdn and rms>0)
        Is=[]
        for n in range(len(I)):
            Is.append(str(I[n]))
        if (len(Is)):
                chan=','.join(Is)
        else:
                chan=''
        imcontsub(imagename=outfile, linefile=outfile+'.line', contfile=outfile+'.cont', fitorder=fit_order,chans=chan)
        ia.open(outfile+'.cont')
        #imcollapse(imagename=outfile+'.cont',axes=[3],chans='0~'+str(ia.shape()[3]/2-1),outfile=outfile+'.cont.1',function='mean')
        #imcollapse(imagename=outfile+'.cont',axes=[3],chans=str(ia.shape()[3]/2)+'~'+str(ia.shape()[3]-1),outfile=outfile+'.cont.2',function='mean')
        imsubimage(imagename=outfile+'.cont', chans='0~0', outfile=outfile+'.cont.1', overwrite=True)
        imsubimage(imagename=outfile+'.cont', chans=str(ia.shape()[3]-1)+'~'+str(ia.shape()[3]-1), outfile=outfile+'.cont.2', overwrite=True)
        ia.close()
        final = ia.imageconcat(infiles=[outfile+'.cont.1',outfile+'.cont.2'], outfile=outfile+'.cont', relax=True, overwrite=True)
        final.done()
        ### OR ###-- still averages data
        ### imrebin(imagename=outfile+'.cont',outfile=outfile+'.cont.2ch',factor=[1,1,1,ia.shape()[3]/2])
        ### OR ###
        ### imregrid(imagename=outfile+'.cont',outfile=outfile+'.cont.2ch',axes=[3],shape=[shp[0],shp[1],shp[2],2]
        ### OR ###
        ### imsubimage(imagename=outfile+'.cont',outfile=outfile+'.cont.2ch',axes=[3],shape=[shp[0],shp[1],shp[2],2]
        # ia.open(out_filename)
        # ia.statistics(verbose=True,axes=[0,1])
        # ia.close()
        ###  could save outfile+'.cont',outfile+'.line', rather than outfile ###
        if build_fits == 'yes':
            exportfits(imagename=outfile+'.line', fitsimage='{0}.fits'.format(outfile))
    except Exception:
        LOG.exception('*********\nConcatenate exception: \n***********')

if __name__ == "__main__":
    args = parse_args()
    LOG.info(args)

    do_imageconcat(
        args.arguments[0],
        args.arguments[1],
        int(args.arguments[2]),
        args.arguments[3],
        args.arguments[4:])
