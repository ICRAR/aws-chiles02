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
Build the physical graph
"""
import boto3

from aws_chiles02.apps_clean import DockerClean, CopyCleanFromS3
from aws_chiles02.common import get_module_name
from aws_chiles02.build_graph_common import AbstractBuildGraph
from aws_chiles02.settings_file import CONTAINER_CHILES02


class CarryOverDataFixMeasurementSet:
    def __init__(self):
        self.s3_out = None


class BuildGraphFindBadMeasurementSet(AbstractBuildGraph):
    def __init__(self, bucket_name, volume, parallel_streams, node_details, shutdown, width, bottom_frequency, session_id, dim_ip):
        super(BuildGraphFindBadMeasurementSet, self).__init__(bucket_name, shutdown, node_details, volume, session_id, dim_ip)
        self._parallel_streams = parallel_streams
        self._s3_uvsub_name = 'uvsub_{0}'.format(width)
        self._min_frequency = bottom_frequency
        self._max_frequency = bottom_frequency + width
        self._list_ip = []
        self._node_index = 0

    def new_carry_over_data(self):
        return CarryOverDataFixMeasurementSet()

    def build_graph(self):
        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)
        self._bucket = s3.Bucket(self._bucket_name)

        # Get all the cleaned files names
        s3_objects = []
        prefix = '{0}/{1}_{2}/'.format(
            self._s3_uvsub_name,
            self._min_frequency,
            self._max_frequency,
        )
        for key in self._bucket.objects.filter(Prefix=prefix):
            if key.key.endswith('.tar'):
                s3_objects.append(key.key)

        self._build_node_map()
        node_id = self._get_next_node()
        count_on_node = 0

        for s3_object in s3_objects:
            self._build_clean_chain(s3_object, count_on_node, node_id)

            count_on_node += 1
            if count_on_node >= self._parallel_streams:
                count_on_node = 0
                node_id = self._get_next_node()

        self.copy_logfiles_and_shutdown()

    def _get_next_node(self):
        next_node = self._list_ip[self._node_index]
        self._node_index += 1
        if self._node_index >= len(self._list_ip):
            self._node_index = 0

        return next_node

    def _build_node_map(self):
        self._list_ip = []
        for key, values in self._node_details.iteritems():
            for value in values:
                self._list_ip.append(value['ip_address'])

    def _build_clean_chain(self, s3_object, count_on_node, node_id):
        # Get the carry over
        carry_over_data = self._map_carry_over_data[node_id]
        if carry_over_data.s3_out is None:
            carry_over_data.s3_out = [None] * self._parallel_streams

        elements = s3_object.split('/')
        s3_drop = self.create_s3_drop(
            node_id,
            self._bucket_name,
            s3_object,
            'aws-chiles02',
            oid='s3_in',
        )
        self._start_oids.append(s3_drop['uid'])

        copy_from_s3 = self.create_app(
            node_id,
            get_module_name(CopyCleanFromS3),
            'app_copy_from_s3',
            min_frequency=self._min_frequency,
            max_frequency=self._max_frequency,
        )
        measurement_set = self.create_directory_container(
            node_id,
            'dir_in_ms_{0}'.format(elements[2])
        )

        # The order of arguments is important so don't put anything in front of these
        copy_from_s3.addInput(s3_drop)
        copy_from_s3.addOutput(measurement_set)

        if carry_over_data.s3_out[count_on_node] is not None:
            copy_from_s3.addInput(carry_over_data.s3_out[count_on_node])

        casa_py_clean_drop = self.create_docker_app(
            node_id,
            get_module_name(DockerClean),
            'app_clean',
            CONTAINER_CHILES02,
            'clean',
            min_frequency=self._min_frequency,
            max_frequency=self._max_frequency,
            iterations=1,
            measurement_sets=[measurement_set['dirname']],
        )
        elements = s3_object.split('/')
        result = self.create_directory_container(
            node_id,
            'dir_clean_output_{0}'.format(elements[2])
        )
        casa_py_clean_drop.addInput(measurement_set)
        casa_py_clean_drop.addOutput(result)

        carry_over_data.s3_out[count_on_node] = result
