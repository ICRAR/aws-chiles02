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
import logging

from casa_code.echo import echo

LOG = logging.getLogger(__name__)

SPECTRAL_WINDOW_ID = 0
NUMBER_CHANNELS = 2
START_FREQUENCY = 4
CHANNEL_WIDTH = 5
WINDOW_WIDTH = 6


@echo
def freq_map(low_frequency, high_frequency, json_data):
    """
    Return the Spectral Window Required given the lower and upper bounds requested.

    2 MHz buffer is added to these values.
    2MHz is equiv to 40km/s, which covers the range of calculated observatory velocities of ~+-30km/s
    """
    spectral_window_low = 0
    spectral_window_high = 14
    number_channels = None
    width_frequency = None
    low_frequency_minus_2 = low_frequency - 2
    high_frequency_plus_2 = high_frequency + 2

    for window in json_data:
        if number_channels is None or width_frequency is None:
            number_channels = int(window[NUMBER_CHANNELS])
            width_frequency = float(window[CHANNEL_WIDTH])

        window_width = float(window[WINDOW_WIDTH]) / 1000.0
        if low_frequency_minus_2 <= float(window[START_FREQUENCY]) + window_width:
            spectral_window_low = window[SPECTRAL_WINDOW_ID]
            break

    for window in json_data:
        window_width = float(window[WINDOW_WIDTH]) / 1000.0
        if high_frequency_plus_2 <= float(window[START_FREQUENCY]) + window_width:
            spectral_window_high = window[SPECTRAL_WINDOW_ID]
            break

    spw = '{}~{}'.format(spectral_window_low, spectral_window_high)
    return spw, number_channels, width_frequency


if __name__ == "__main__":
    import doctest

    doctest.testmod()
