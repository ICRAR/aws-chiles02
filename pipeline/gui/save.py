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
Each time the program is closed, we want to save whatever was currently configured, and keep up to the last X
number of saved configs.
"""

from abc import *


class BaseChilesGUIConfig:
    __metaclass__ = ABCMeta

    @abstractmethod
    def save(self, new_values):
        pass

    @abstractmethod
    def load(self):
        pass


class NullChilesGUIConfig:

    def save(self, new_values):
        print "Saving new values"
        print new_values

    def load(self):
        print "Loading values"
        return {}