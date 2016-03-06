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
import uuid
from abc import ABCMeta, abstractmethod


class AbstractBuildGraph:
    # This ensures that:
    #  - This class cannot be instantiated
    #  - Subclasses implement methods decorated with @abstractmethod
    __metaclass__ = ABCMeta

    def __init__(self, bucket_name, shutdown, node_details, volume):
        self._drop_list = []
        self._start_oids = []
        self._map_carry_over_data = {}
        self._bucket_name = bucket_name
        self._shutdown = shutdown
        self._bucket = None
        self._node_details = node_details
        self._volume = volume
        self._counters = {}

        for key, list_ips in self._node_details.iteritems():
            for instance_details in list_ips:
                self._map_carry_over_data[instance_details['ip_address']] = self.new_carry_over_data()

    @property
    def drop_list(self):
        return self._drop_list

    @property
    def start_oids(self):
        return self._start_oids

    def append(self, drop):
        self._drop_list.append(drop)

    def get_oid(self, count_type):
        count = self._counters.get(count_type)
        if count is None:
            count = 1
        else:
            count += 1
        self._counters[count_type] = count

        return '{0}__{1:06d}'.format(count_type, count)

    @staticmethod
    def get_uuid():
        return str(uuid.uuid4())

    @abstractmethod
    def build_graph(self):
        """
        Build the graph
        """

    @abstractmethod
    def new_carry_over_data(self):
        """"
        Get the carry over data structure
        """

    @abstractmethod
    def add_shutdown(self):
        """"
        Add the shutdown drop
        """
