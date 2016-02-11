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
The abstract graph builder
"""
from abc import ABCMeta, abstractmethod


class AbstractBuildGraph:
    # This ensures that:
    #  - This class cannot be instantiated
    #  - Subclasses implement methods decorated with @abstractmethod
    __metaclass__ = ABCMeta

    def __init__(self, command_line_args):
        self._drop_list = []
        self._start_oids = []
        self._volume = command_line_args.volume
        self._cores = command_line_args.cores
        self._width = command_line_args.width
        self._nodes = command_line_args.nodes
        self._bucket_name = command_line_args.bucket
        self._shutdown = command_line_args.shutdown
        self._bucket = None

    @property
    def drop_list(self):
        return self._drop_list

    @property
    def start_oids(self):
        return self._start_oids

    def append(self, drop):
        self._drop_list.append(drop)

    @abstractmethod
    def build_graph(self):
        """
        Build the graph
        """
