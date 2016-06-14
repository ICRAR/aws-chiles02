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

from datetime import datetime
from sqlalchemy import create_engine

from casa_code.casa_common import parse_args
from casa_code.database import VISSTAT_META, VISSTAT
from casa_code.echo import echo

casalog.filter('DEBUGGING')
logging.info('Starting logger for...')
LOG = logging.getLogger('stats')


class DataToStore(object):
    def __init__(self, scan_number, spectral_window_number, channel_number, stats):
        self.scan_number = scan_number
        self.spectral_window_number = spectral_window_number
        self.channel_number = channel_number
        self.stats = stats


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
        scan_summary = ms.getscansummary()
        scans = scan_summary.keys()

        # This assumes all spw have the same no channels as the first
        spectral_window_info = ms.getspectralwindowinfo()
        number_sceptal_windows = len(spectral_window_info)
        number_channels = spectral_window_info['0']['NumChan']
        ms.close()
        results = []

        # This will fail if there is no data
        zerov = visstat(
            vis=in_ms,
            datacolumn='data',
            scan=str(scans[0]),
            spw='0:0',
            useflags=False
        )

        # strip off the ['DATA']
        zerov = zerov[zerov.keys()[0]]
        for k in zerov.keys():
            zerov[k] = 0

        # TODO for scan_number in scans:
        for scan_number in scans[:2]: # TODO:
            for spectral_window_number in range(0, number_sceptal_windows):
                for channel_number in range(0, number_channels):
                    vis_stats = visstat(
                        vis=in_ms,
                        datacolumn='data',
                        scan=scan_number,
                        spw=str(spectral_window_number) + ':' + str(channel_number)
                    )
                    if spectral_window_info is None:
                        results.append(DataToStore(scan_number, spectral_window_number, channel_number, zerov))
                    else:
                        # strip off the ['CORRECTED']
                        results.append(DataToStore(scan_number, spectral_window_number, channel_number, vis_stats[vis_stats.keys()[0]]))

        return results

    except Exception:
        LOG.exception('*********\nStats exception: \n***********')
        return None


@echo
def store_stats(results, password, db_hostname, day_name_id, width, min_frequency, max_frequency):
    db_login = "mysql+pymysql://root:{0}@{1}/chiles02".format(password, db_hostname)
    engine = create_engine(db_login)
    connection = engine.connect()
    transaction = connection.begin()
    try:
        sql_result = connection.execute(
            VISSTAT_META.insert(),
            day_name_id=day_name_id,
            width=width,
            min_frequency=min_frequency,
            max_frequency=max_frequency,
            update_time=datetime.now()
        )
        visstat_meta_id = sql_result.inserted_primary_key[0]

        for result in results:
            connection.execute(
                VISSTAT.insert(),
                visstat_meta_id=visstat_meta_id,
                scan=result.scan_number,
                spectral_window=result.spectral_window_number,
                channel=result.channel_number,
                max=result.stats['max'],
                mean=result.stats['mean'],
                medabsdevmed=result.stats['medabsdevmed'],
                median=result.stats['median'],
                min=result.stats['min'],
                npts=result.stats['npts'],
                quartile=result.stats['quartile'],
                rms=result.stats['rms'],
                stddev=result.stats['stddev'],
                sum=result.stats['sum'],
                sumsq=result.stats['sumsq'],
                var=result.stats['var'],
                update_time=datetime.now()
            )
        transaction.commit()
    except Exception:
        LOG.exception('Insert error')
        if transaction is not None:
            transaction.rollback()


args = parse_args()
LOG.info(args)

results = do_stats(args.arguments[0])
if results is not None:
    store_stats(
        results,
        args.arguments[1],
        args.arguments[2],
        args.arguments[3],
        args.arguments[4],
        args.arguments[5],
        args.arguments[6],
    )
