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
Perform the GainCal
"""
import json
import logging
import os
import shutil

from casa_code.casa_common import find_file, parse_args
from casa_code.freq_map import freq_map

casalog.filter('DEBUGGING')
logging.info('Starting logger for...')
LOG = logging.getLogger('gaincal')


def do_calibration(infile, out_dir,  out_pngs, apply_cal, w_projection_planes, number_taylor_terms, model):
    """
    Perform the GAINCAL step
    inputs: infile=visibility, out_dir=dest, out_pngs=False/True, apply_cal=False/True, w_projection_planes=integer, 
            number_taylor_terms=intege, model=list of strings

    Task will run gaincal on whole dataset, on 30min and on scan (~10min)

    out_pngs uses plotcal for each gaincal solution, apply_cal will apply the 30m solution

    w_projection_planes is needed for the model calculation (I believe). 
    num_taylor_terms should match the number of models (i.e. not included the HA binning as yet)
    """

    if out_pngs == 'yes':
        png_directory = os.path.join(out_dir, 'qa_pngs')
        if not os.path.exists(png_directory):
            os.makedirs(png_directory)
        
    tb.open(infile)
    col_list=tb.colnames()
    tb.close()
    ntt=len(model)
    if (number_taylor_terms!=len(model)):
            print 'Models and requested Taylor terms do not match: '+str(number_taylor_terms)

    try:
            if (len(model)>0):
                    if ('MODEL' in col_list):
                        tb.open(infile,nomodify=False)
                        tb.removecols('MODEL')
                        tb.close()
                    # Create/Flush model_data column
                    im.open(thems=infile, usescratch=True)
                    # Select all data in this case
                    im.selectvis()
                    # These are the parameters for the generation of the model
                    # Not sure how many of them are important here -- all except mode?
                    im.defineimage(
                            nx=4096,
                            ny=4096,
                            cellx='2arcsec',
                            celly='2arcsec',
                            mode='mfs',
                            facets=1)
                    im.setoptions(ftmachine='wproject', wprojplanes=w_projection_planes)
                    # Find the refernce frequency and set no. taylor terms
                    ms.open(infile)
                    fq = ms.getspectralwindowinfo()['0']['RefFreq']
                    ms.close()
                    im.settaylorterms(ntaylorterms=ntt, reffreq=fq)
                    #
                    print 'Models in this pass: '+str(model)
                    #
                    im.ft(model=model[0:ntt], incremental=False)
                    im.close()
            if (apply_cal):
                    if ('CORRECTED_DATA' in col_list):
                        tb.open(infile,nomodify=False)
                        tb.removecols('CORRECTED_DATA')
                        tb.close()
            # gaincal()
            gaincal(
                vis=infile,
                caltable=infile.replace('ms','.phase.inf.cal'),
                selectdata=False,
                solint='inf',
                combine='scan',
                minsnr='3.0',
                calmode='p'
            )
            if out_pngs:
                    plotcal(
                        figfile=png_directory+'/'+infile.replace('ms','.phase.inf.png'),
                        caltable=infile.replace('ms','.phase.inf.cal'),
                        showgui=False,
                        yaxis='phase'
                )
            gaincal(
                vis=infile,
                caltable=infile.replace('ms','.phase.30m.cal'),
                selectdata=False,
                solint='1800s',
                combine='scan',
                minsnr='3.0',
                calmode='p'
            )
            if out_pngs:
                    plotcal(
                        figfile=png_directory+'/'+infile.replace('ms','.phase.30m.png'),
                        caltable=infile.replace('ms','.phase.30m.cal'),
                        showgui=False,
                        yaxis='phase')
            gaincal(
                vis=infile,
                caltable=infile.replace('ms','.phase.scan.cal'),
                selectdata=False,
                solint='inf',
                combine='',
                minsnr='3.0',
                calmode='p'
            )
            if out_pngs:
                    plotcal(
                        figfile=png_directory+'/'+infile.replace('ms','.phase.scan.png'),
                        caltable=infile.replace('ms','.phase.scan.cal'),
                        showgui=False,
                        yaxis='phase')
            if apply_cal:
                    applycal(
                        vis=infile,
                        gaintable=infile.replace('ms','.phase.30m.cal'),
                        selectdata=False)

    except Exception:
            LOG.exception('*********\ngaincal exception:\n***********')


if __name__ == "__main__":
    args = parse_args()
    LOG.info(args)

    do_calibration(
        infile=args.arguments[0],
        out_dir=args.arguments[1],
        out_pngs=args.arguments[2],
        apply_cal=args.arguments[3],
        w_projection_planes=int(args.arguments[4]),
        number_taylor_terms=int(args.arguments[5]),
        model=args.arguments[6:]
    )
