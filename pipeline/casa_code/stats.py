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
Perform the Statistics Calc
"""
import logging
import os

from casa_code.casa_common import parse_args
from casa_code.echo import echo

casalog.filter('DEBUGGING')
LOG = logging.getLogger(__name__)


@echo
def do_stats(in_ms, out_result)
    """
    Performs the Stats extraction
    Inputs: VIS_name (str), Output (str)

    example:
     do_stats('vis_1400~1404','vis_1400~1404.stats')

    """

    LOG.info('stats(vis={0},  output={2})'.format(in_ms, out_result))
    try:
        ms.open(in_ms)
        SI=ms.getscansummary()
        l=SI.keys()
        l2=[]
        for n in range(0,len(l)):
            l2.append(int(l[n]))
        l2.sort()
        l=l2
        # This assumes all spw have the same no channels as the first
        l2=ms.getspectralwindowinfo()
        num_spw=len(l2)
        num_chan=l2['0']['NumChan']
        ms.close()
        rms=[]
        #This will fail is there is no data
        zerov=visstat(vis=in_ms,datacolumn='data',
                      scan=str(l[0]),spw='0:0',useflags=F)
        #strip off the ['DATA']
        zerov=zerov[zerov.keys()[0]]
        for k in zerov.keys():
            zerov[k]=0

        for ns in range(0,len(l)):
            for nsp in range(0,num_spw):
                for nch in range(0,num_chan):
                    l2=visstat(vis=vis,datacolumn='data',
                               scan=str(l[ns]),spw=str(nsp)+':'+str(nch))
                    if (l2==None):
                        rms.append(zerov)
                    else:
                        #strip off the ['CORRECTED']
                        rms.append(l2[l2.keys()[0]])

        out_result=rms
    except Exception:
        LOG.exception('*********\nStats exception: \n***********')


args = parse_args()
LOG.info(args)

do_stats(
        args.arguments[0],
        args.arguments[1])
