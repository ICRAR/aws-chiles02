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
A fake logger for CASA
"""
import sys


class CasaLogger(object):
    def __init__(self, origin):
        self._origin = origin

    def info(self, message):
        self._post(message, priority="INFO")

    def warning(self, message):
        self._post(message, priority="WARNING")

    def exception(self, message):
        exception_info = sys.exc_info()
        self._post('{0}\n{1}'.format(message, exception_info), priority="SEVERE")

    def _post(self, message, priority):
        print '{0}:{1}:{2}'.format(self._origin, priority, message)
