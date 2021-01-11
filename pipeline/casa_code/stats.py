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
import csv
import logging

from casa_code.casa_common import parse_args
from casa_code.echo import echo

# casalog.filter('DEBUGGING')
logging.info('Starting logger for...')
LOG = logging.getLogger('stats')


@echo
def do_stats(in_ms_list, out_csv_file, observation):
    """
    Performs the Stats extraction
    Inputs: VIS_name (str), Output (str)

    example:
     do_stats('vis_1400~1404','vis_1400~1404.stats')
    """
    LOG.info('stats(vis={0}, out_csv_file={1})'.format(in_ms_list, out_csv_file))
    try:
      for in_ms in in_ms_list.split(','):
        ms.open(in_ms)
        scan_summary = ms.getscansummary()
        scans = scan_summary.keys()

        # This assumes all spw have the same no channels as the first
        spectral_window_info = ms.getspectralwindowinfo()
        number_spectral_windows = len(spectral_window_info)
        number_channels = np.zeros(number_spectral_windows)
        for spectral_window_number in range(number_spectral_windows):
            number_channels[spectral_window_number] = spectral_window_info[str(spectral_window_number)]['NumChan']
        ms.close()

        # This will fail if there is no data
        zerov = visstat(
            vis=in_ms,
            axis='real',
            datacolumn='data',
            scan=str(scans[0]),
            spw='0:0',
            useflags=False
        )

        # strip off the ['DATA']
        zerov = zerov[zerov.keys()[0]]
        for k in zerov.keys():
            zerov[k] = 0

        with open(out_csv_file, 'ab') as csv_file:
            csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow([
                'observation',
                'scan',
                'begin_time',
                'end_time',
                'spectral_window',
                'channel',
                'max',
                'mean',
                'medabsdevmed',
                'median',
                'min',
                'npts',
                'firstquartile',
                'thirdquartile',
                'rms',
                'stddev',
                'sum',
                'sumsq',
                'var',
            ])
            for scan_number in scans:
                begin_time = scan_summary[scan_number]['0']['BeginTime']
                end_time = scan_summary[scan_number]['0']['EndTime']
                for spectral_window_number in range(0, number_spectral_windows):
                    for channel_number in range(0, number_channels[spectral_window_number]):
                        vis_stats = visstat(
                            vis=in_ms,
                            datacolumn='data',
                            scan=scan_number,
                            spw=str(spectral_window_number) + ':' + str(channel_number)
                        )
                        if vis_stats is None:
                            try:
                                write_line(csv_writer, observation, scan_number, begin_time, end_time, spectral_window_number, channel_number, zerov)
                            except:
                                LOG.warning('WriteLine Failed!')
                        else:
                            # strip off the ['CORRECTED']
                            try:
                                write_line(csv_writer, observation, scan_number, begin_time, end_time, spectral_window_number, channel_number, vis_stats[vis_stats.keys()[0]])
                            except:
                                LOG.warning('WriteLine Failed!')


    except Exception:
        LOG.exception('*********\nStats exception: \n***********')
        return None


def write_line(csv_writer, observation, scan_number, begin_time, end_time, spectral_window_number, channel_number, result):
    csv_writer.writerow([
        observation,
        scan_number,
        begin_time,
        end_time,
        spectral_window_number,
        channel_number,
        result['max'],
        result['mean'],
        result['medabsdevmed'],
        result['median'],
        result['min'],
        result['npts'],
        result['firstquartile'],
        result['thirdquartile'],
        result['rms'],
        result['stddev'],
        result['sum'],
        result['sumsq'],
        result['variance'],
    ])


if __name__ == "__main__":
    args = parse_args()
    LOG.info(args)

    do_stats(args.arguments[0], args.arguments[1], args.arguments[2])
