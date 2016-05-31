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

from casa_code.casa_common import parse_args
from casa_code.echo import echo

casalog.filter('DEBUGGING')
LOG = logging.getLogger(__name__)


@echo
def do_uvsub(in_dir, outfile, model):
    """
    Performs the UVSUB step
     Use imtool to fill MODEL_DATA correctly
     runs UVSUB to fill CORRECTED_DATA (=DATA-MODEL_DATA)
     make uvsub_XX from vis_XX and column CORRECTED_DATA
    Inputs: VIS_name (str), MODEL (list of str)
    Number of taylor terms is length of model list. Expecting 2

    example:
     do_uvsub('vis_1400~1404',['Epoch1_Images_Wproject/epoch1.mmstest_spw_14.model.tt0','Epoch1_Images_Wproject/epoch1.mmstest_spw_14.model.tt1'])


     I suggest that SPW in the model strings is deduced from
     spw=int(((freq_min+freq_max)/2-946)/32)
    """

    LOG.info('uvsub(vis={0}, model={1}, output={2})'.format(in_dir, str(model), outfile))
    try:
        # dump_all()
        #
        #  Here for reference; not needed as no CD column
        # tb.open(vis_in_dir,nomodify=False)
        # tb.removecols('CORRECTED_DATA')
        # tb.close()
        #
        # Create/Flush model_data column
        im.open(thems=in_dir, addmodel=True)

        # Select all data in this case
        im.selectvis()
        # These are the parameters for the generation of the model
        # Not sure how many of them are important here -- all except mode?
        im.defineimage(nx=2048, ny=2048, cellx='1.5arcsec',
                       celly='1.5arcsec', mode='mfs', facets=1)
        im.setoptions(ftmachine='wproject', wprojplanes=12)
        # Find the refernce frequency and set no. taylor terms
        ms.open(in_dir)
        fq = ms.getspectralwindowinfo()['0']['RefFreq']
        ms.close()
        im.settaylorterms(ntaylorterms=len(model), reffreq=fq)
        #
        im.ft(model=model, incremental=False)
        im.close()
        # Now do the subtraction
        uvsub(vis=in_dir, reverse=False)
        split(vis=in_dir, outputvis=outfile, datacolumn='corrected')
    except Exception:
        LOG.exception('*********\nUVSub exception: \n***********')


args = parse_args()
LOG.info(args)

do_uvsub(
        args.arguments[0],
        args.arguments[1],
        args.arguments[2:])
