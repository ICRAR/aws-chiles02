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

from datetime import time
from sqlalchemy import create_engine, select

from casa_code.casa_common import parse_args
from casa_code.database import VISSTAT_META, DAY_NAME, VISSTAT
from casa_code.echo import echo

casalog.filter('DEBUGGING')
LOG = logging.getLogger(__name__)


@echo
def do_stats(in_ms):
    """
    Performs the Stats extraction
    Inputs: VIS_name (str), Output (str)

    example:
     do_stats('vis_1400~1404','vis_1400~1404.stats')
    """
    LOG.info('stats(vis={0})'.format(in_ms))
    try:
        ms.open(in_ms)
        SI = ms.getscansummary()
        l = SI.keys()

        l2 = []
        for n in range(0, len(l)):
            l2.append(int(l[n]))
        l2.sort()
        l = l2

        # This assumes all spw have the same no channels as the first
        l2 = ms.getspectralwindowinfo()
        num_spw = len(l2)
        num_chan = l2['0']['NumChan']
        ms.close()
        rms = []

        # This will fail if there is no data
        zerov = visstat(
            vis=in_ms,
            datacolumn='data',
            scan=str(l[0]),
            spw='0:0',
            useflags=False
        )

        # strip off the ['DATA']
        zerov = zerov[zerov.keys()[0]]
        for k in zerov.keys():
            zerov[k] = 0

        for ns in range(0, len(l)):
            for nsp in range(0, num_spw):
                for nch in range(0, num_chan):
                    l2 = visstat(
                        vis=in_ms,
                        datacolumn='data',
                        scan=str(l[ns]),
                        spw=str(nsp) + ':' + str(nch)
                    )
                    if l2 is None:
                        rms.append(zerov)
                    else:
                        # strip off the ['CORRECTED']
                        rms.append(l2[l2.keys()[0]])

        return rms, len(l), num_spw, num_chan

    except Exception:
        LOG.exception('*********\nStats exception: \n***********')
        return None


def store_stats(results, password, db_hostname, day_name, min_frequency, max_frequency, number_SI, number_spectral_windows, number_channels):
    db_login = "mysql+pymysql://root:{0}@{1}/aws_chiles02".format(password, db_hostname)
    engine = create_engine(db_login)
    connection = engine.connect()
    transaction = connection.begin()
    try:
        day_name_id = connection.execute(
            select([DAY_NAME.c.day_name_id]).where(DAY_NAME.c.name == day_name)
        ).fetchone()[0]

        sql_result = connection.execute(
            VISSTAT_META.insert(),
            day_name_id=day_name_id,
            min_frequency=min_frequency,
            max_frequency=max_frequency,
            number_SI=number_SI,
            number_spectral_windows=number_spectral_windows,
            number_channels=number_channels,
            update_time=time.now()
        )
        visstat_meta_id = sql_result.inserted_primary_key[0]

        for result in results:
            connection.execute(
                VISSTAT.insert(),
                visstat_meta_id=visstat_meta_id,
                sequence=result['sequence'],
                max=result['max'],
                mean=result['mean'],
                medabsdevmed=result['medabsdevmed'],
                median=result['median'],
                min=result['min'],
                npts=result['npts'],
                quartile=result['quartile'],
                rms=result['rms'],
                stddev=result['stddev'],
                sum=result['sum'],
                sumsq=result['sumsq'],
                var=result['var'],
                update_time=time.now()
            )
        transaction.commit()
    except Exception:
        LOG.exception('Insert error')
        if transaction is not None:
            transaction.rollback()


args = parse_args()
LOG.info(args)

results, number_SI, number_spectral_windows, number_channels = do_stats(args.arguments[0])
if results is not None:
    store_stats(
        results,
        args.arguments[1],
        args.arguments[2],
        args.arguments[3],
        args.arguments[4],
        args.arguments[5],
        number_SI,
        number_spectral_windows,
        number_channels
    )
