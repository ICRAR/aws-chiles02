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
Create a CSV of statistics from the Measurement set
"""

from astroplan import Observer
from astropy.coordinates import EarthLocation, SkyCoord
from astropy.time import Time

from casa_code.casa_logging import CasaLogger

LOG = CasaLogger(__name__)


class GetStatistics(object):
    def __init__(self, measurement_set):
        self._measurement_set = measurement_set
        self._spectral_window_info = None
        self._observer = None

        self._field_details = {}

    def extract_statistics(self, database_visitor, observation_id):
        # Open the measurement set
        ms.open(self._measurement_set)
        summary = ms.summary()

        number_fields = summary['nfields']
        time_ref = summary['timeref']

        for field_id in range(0, int(number_fields)):
            self._field_details[field_id] = ms.getfielddirmeas(fieldid=field_id)

        self._spectral_window_info = ms.getspectralwindowinfo()

        scan_summary = ms.getscansummary()
        ms.close()

        # Create the VLA observatory
        vla = EarthLocation.from_geodetic('107d37m05.819s', '34d04m43.497s', 2124)
        self._observer = Observer(location=vla, timezone=time_ref)

        for scan_number in scan_summary.keys():
            begin_time = scan_summary[scan_number]['0']['BeginTime']
            end_time = scan_summary[scan_number]['0']['EndTime']
            field_id = scan_summary[scan_number]['0']['FieldId']

            hour_angle_begin_time = self._get_hour_angle(begin_time, field_id)
            hour_angle_end_time = self._get_hour_angle(end_time, field_id)

            for spectral_window_number in self._spectral_window_info.keys():
                number_channels = self._spectral_window_info[spectral_window_number]['NumChan']
                for channel_number in range(0, number_channels):
                    vis_stats = visstat(
                        vis=self._measurement_set,
                        datacolumn='data',
                        scan=scan_number,
                        spw='{0}:{1}'.format(spectral_window_number, channel_number),
                        useflags=False,
                    )
                    if vis_stats is not None:
                        frequency = self._get_frequency(spectral_window_number, channel_number)
                        result = vis_stats[vis_stats.keys()[0]]
                        database_visitor.writerow(
                            observation_id,
                            scan_number,
                            begin_time,
                            end_time,
                            hour_angle_begin_time,
                            hour_angle_end_time,
                            spectral_window_number,
                            channel_number,
                            frequency,
                            result['max'],
                            result['mean'],
                            result['medabsdevmed'],
                            result['median'],
                            result['min'],
                            result['npts'],
                            result['quartile'],
                            result['rms'],
                            result['stddev'],
                            result['sum'],
                            result['sumsq'],
                            result['var'],
                        )

    def _get_frequency(self, spectral_window_number, channel_number):
        spectral_window = self._spectral_window_info[spectral_window_number]
        frequency = spectral_window['Chan1Freq']
        width = spectral_window['ChanWidth']

        return frequency + channel_number * width

    def _get_hour_angle(self, time_mjd, field_id):
        time = Time(time_mjd, format='mjd')
        right_ascension = self._field_details[field_id]['m0']
        declination = self._field_details[field_id]['m1']
        target = SkyCoord(right_ascension['value'], declination['value'], unit=right_ascension['unit'])
        return self._observer.target_hour_angle(time, target)
