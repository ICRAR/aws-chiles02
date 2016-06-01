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
from aws_chiles02.apps_general import CleanupDirectories
from aws_chiles02.apps_uvsub import CopyUvsubFromS3, DockerUvsub, CopyUvsubToS3
from aws_chiles02.common import get_module_name
from aws_chiles02.build_graph_common import AbstractBuildGraph
from aws_chiles02.settings_file import CONTAINER_CHILES02


class CarryOverDataUvsub:
    def __init__(self):
        self.memory_drop_list = None


class BuildGraphUvsub(AbstractBuildGraph):
    def __init__(self, work_to_do, bucket_name, volume, parallel_streams, node_details, shutdown, width, session_id):
        super(BuildGraphUvsub, self).__init__(bucket_name, shutdown, node_details, volume, session_id)
        self._work_to_do = work_to_do
        self._parallel_streams = parallel_streams
        self._s3_uvsub_name = 'uvsub_{0}'.format(width)
        self._s3_split_name = 'split_{0}'.format(width)
        self._list_ip = []
        self._node_index = 0

    def new_carry_over_data(self):
        return CarryOverDataUvsub()

    def build_graph(self):
        self._build_node_map()

        # Add the start drops
        node_id = self._get_next_node()
        count_on_node = 0
        for split_to_process in self._work_to_do[:5]:   # TODO: Remove after testing
            self._build_uvsub_chain(split_to_process, count_on_node, node_id)

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

    def _build_uvsub_chain(self, split_to_process, count_on_node, node_id):
        s3_drop = self.create_s3_drop(
            node_id,
            self._bucket_name,
            '{0}/{1}/{2}'.format(
                self._s3_split_name,
                split_to_process[0],
                split_to_process[1],
            ),
            'aws-chiles02',
            oid='s3_in',
        )

        frequencies = split_to_process[0].split('_')
        copy_from_s3 = self.create_app(
            node_id,
            get_module_name(CopyUvsubFromS3),
            'app_copy_from_s3',
            min_frequency=frequencies[0],
            max_frequency=frequencies[1],

        )
        measurement_set = self.create_directory_container(
            node_id,
            'dir_in_ms'
        )

        # Work with the carry over
        carry_over_data = self._map_carry_over_data[node_id]
        if carry_over_data.memory_drop_list is None:
            carry_over_data.memory_drop_list = [None] * self._parallel_streams

        if carry_over_data.memory_drop_list[count_on_node] is None:
            self._start_oids.append(s3_drop['uid'])
        else:
            copy_from_s3.addInput(carry_over_data.memory_drop_list[count_on_node])

        # The order of arguments is important so don't put anything in front of these
        copy_from_s3.addInput(s3_drop)
        copy_from_s3.addOutput(measurement_set)

        # Do the UV subtraction
        casa_py_uvsub_drop = self.create_docker_app(
            node_id,
            get_module_name(DockerUvsub),
            'app_uvsub',
            CONTAINER_CHILES02,
            'uvsub',
            min_frequency=frequencies[0],
            max_frequency=frequencies[1],
        )
        result = self.create_directory_container(node_id, 'dir_uvsub_output')
        casa_py_uvsub_drop.addInput(measurement_set)
        casa_py_uvsub_drop.addOutput(result)

        copy_uvsub_to_s3 = self.create_app(
            node_id,
            get_module_name(CopyUvsubToS3),
            'app_copy_uvsub_to_s3',
            min_frequency=frequencies[0],
            max_frequency=frequencies[1],
        )
        s3_uvsub_drop_out = self.create_s3_drop(
            node_id,
            self._bucket_name,
            '{0}/{1}/{2}'.format(
                self._s3_uvsub_name,
                split_to_process[0],
                split_to_process[1],
            ),
            'aws-chiles02',
            oid='s3_out',
        )
        copy_uvsub_to_s3.addInput(result)
        copy_uvsub_to_s3.addOutput(s3_uvsub_drop_out)

        clean_up = self.create_app(
            node_id,
            get_module_name(CleanupDirectories),
            'app_cleanup_directories',
        )
        memory_drop = self.create_memory_drop(node_id)
        clean_up.addInput(s3_uvsub_drop_out)
        clean_up.addInput(result)
        clean_up.addInput(measurement_set)
        clean_up.addOutput(memory_drop)

        # Remember the end of the tail
        carry_over_data.memory_drop_list[count_on_node] = memory_drop
