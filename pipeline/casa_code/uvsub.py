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
Perform the UV Subtraction
"""
import logging
import os

from casa_code.casa_common import parse_args
from casa_code.echo import echo

casalog.filter('DEBUGGING')
logging.info('Starting logger for...')
LOG = logging.getLogger('uvsub')


@echo
def do_uvsub(in_dir, out_dir, out_ms, w_projection_planes, number_taylor_terms, model):
    """
    Performs the UVSUB step
     Use imtool to fill MODEL_DATA correctly
     runs UVSUB to fill CORRECTED_DATA (=DATA-MODEL_DATA)
     make uvsub_XX from vis_XX and column CORRECTED_DATA
    Inputs: VIS_name (str), MODEL (list of str)
    Number of taylor terms is length of model list. Expecting 2

    example:
     do_uvsub('vis_1400~1404',['Epoch1_Images_Wproject/epoch1.bb.gt4k.si_spw_14.model.tt0','Epoch1_Images_Wproject/epoch1.bb.gt4k.si_spw_14.model.tt1'])


     I suggest that SPW in the model strings is deduced from
     spw=int(((freq_min+freq_max)/2-946)/32)
    """

    LOG.info(
        'uvsub(vis={0}, model={1}, out_dir={2}, out_ms={3}, w_projection_planes={4})'.format(
            in_dir,
            str(model),
            out_dir,
            out_ms,
            w_projection_planes))
    try:
        # dump_all()
        #
        #  Here for reference; not needed as no CD column
        # tb.open(vis_in_dir,nomodify=False)
        # tb.removecols('CORRECTED_DATA')
        # tb.close()
        #
        # Create/Flush model_data column
        im.open(thems=in_dir, usescratch=True)

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
            facets=1
        )
        im.setoptions(ftmachine='wproject', wprojplanes=w_projection_planes)

        # Find the refernce frequency and set no. taylor terms
        ms.open(in_dir)
        fq = ms.getspectralwindowinfo()['0']['RefFreq']
        ms.close()
        # Special steps for outliers
        ntt=len(model)
        if (ntt<number_taylor_terms):
            print 'Requested number of taylor terms: '+str(number_taylor_terms)
            print 'Is less than number of models given: '+str(ntt)
            print 'Setting former to the latter.'
            number_taylor_terms=ntt
        elif (ntt>number_taylor_terms):
          tmp_name=os.path.join(out_dir, out_ms+'.tmp')
          ntt=number_taylor_terms
        print str(len(model))+' models provided. Using '+str(ntt)+' for spectral index subtraction'

        im.settaylorterms(ntaylorterms=ntt, reffreq=fq)

        #
        print 'Models in this pass: '+str(model[0:ntt])
        im.ft(model=model[0:ntt], incremental=False)
        im.close()

        # Now do the subtraction
        uvsub(vis=in_dir, reverse=False)
        # Do we have outliers??
        if (len(model)>ntt):
           print 'Using remaing '+str(len(model)-ntt)+' for outlier subtraction'
           split(vis=in_dir, outputvis=tmp_name, datacolumn='corrected')
           im.open(thems=tmp_name, usescratch=True)
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
               facets=1
           )
           im.setoptions(ftmachine='wproject', wprojplanes=w_projection_planes,freqinterp='linear')
           im.settaylorterms(ntaylorterms=1)
           #
           print 'Models in this pass: '+str(model[ntt:len(model)])
           im.ft(model=model[ntt:len(model)], incremental=False)
           im.close()
           uvsub(vis=tmp_name, reverse=False)
           split(vis=tmp_name, outputvis=os.path.join(out_dir, out_ms), datacolumn='corrected')
        else:
           split(vis=in_dir, outputvis=os.path.join(out_dir, out_ms), datacolumn='corrected')

    except Exception:
        LOG.exception('*********\nUVSub exception: \n***********')


if __name__ == "__main__":
    args = parse_args()
    LOG.info(args)

    do_uvsub(
            in_dir=args.arguments[0],
            out_dir=args.arguments[1],
            out_ms=args.arguments[2],
            w_projection_planes=int(args.arguments[3]),
            number_taylor_terms=int(args.arguments[4]),
            model=args.arguments[5:])
