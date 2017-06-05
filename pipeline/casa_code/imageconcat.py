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

from .casa_common import parse_args
from .echo import echo

casalog.filter('DEBUGGING')
logging.info('Starting logger for...')
LOG = logging.getLogger('imageconcat')


@echo
def do_imageconcat(cube_dir, out_filename, input_files):
    """
    Perform the CONCATENATION step
    :param input_files:
    :param out_filename:
    """
    if not os.path.exists(cube_dir):
        os.makedirs(cube_dir)

    outfile = os.path.join(cube_dir, out_filename)
    LOG.info('imageconcat(vis={0}, imagename={1})'.format(str(input_files), outfile))

    try:
        # IA used to report the statistics to the log file
        # for input_file in input_files:
        #     ia.open(input_file)
        #     ia.statistics(verbose=True,axes=[0,1])
        #     ia.close()

        # ia doesn't need an import - it is just available in casa
        final = ia.imageconcat(infiles=input_files, outfile=outfile, relax=True, overwrite=True)
        final.done()
        imcontsub(imagename=outfile, linefile=outfile+'.line', contfile=outfile+'.cont', fitorder=1)
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
        exportfits(imagename=outfile+'.line', fitsimage='{0}.fits'.format(outfile))
    except Exception:
        LOG.exception('*********\nConcatenate exception: \n***********')

if __name__ == "__main__":
    args = parse_args()
    LOG.info(args)

    do_imageconcat(
        args.arguments[0],
        args.arguments[1],
        args.arguments[2:])
