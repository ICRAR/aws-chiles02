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
import operator
import os

from aws_chiles02.apps_mstransform import CopyMsTransformFromS3, CopyMsTransformToS3, DockerListobs, DockerMsTransform, CasaMsTransform, CasaListobs
from aws_chiles02.build_graph_common import AbstractBuildGraph
from aws_chiles02.common import get_module_name, get_observation, make_groups_of_frequencies
from aws_chiles02.settings_file import CONTAINER_CHILES02, SIZE_1GB


class CarryOverDataMsTransform:
    def __init__(self):
        self.drop_listobs = None
        self.barrier_drop = None


class BuildGraphMsTransform(AbstractBuildGraph):
    def __init__(self, **keywords):
        super(BuildGraphMsTransform, self).__init__(**keywords)
        self._work_to_do = keywords['work_to_do']
        self._parallel_streams = keywords['parallel_streams']
        self._s3_split_name = keywords['split_directory']
        self._use_bash = keywords['use_bash']
        self._observation_phase = keywords['observation_phase']
        self._casa_version = keywords['casa_version']

        # Get a sorted list of the keys
        self._keys = sorted(self._work_to_do.keys(), key=operator.attrgetter('size'))
        self._map_day_to_node = None

    def new_carry_over_data(self):
        return CarryOverDataMsTransform()

    def build_graph(self):
        self._build_node_map()

        for day_to_process in self._keys:
            node_id = self._get_next_node(day_to_process)
            carry_over_data = self._map_carry_over_data[node_id]
            list_frequency_groups = self._work_to_do[day_to_process]
            frequency_groups = make_groups_of_frequencies(list_frequency_groups, self._parallel_streams)

            add_output_s3 = []
            if carry_over_data.drop_listobs is not None:
                add_output_s3.append(carry_over_data.drop_listobs)

            measurement_set, properties, drop_listobs = \
                self._setup_measurement_set(
                    day_to_process,
                    carry_over_data.barrier_drop,
                    add_output_s3,
                    node_id
                )

            carry_over_data.drop_listobs = drop_listobs

            outputs = []
            for group in frequency_groups:
                last_element = None
                for frequency_pairs in group:
                    last_element = self._split(
                        last_element,
                        frequency_pairs,
                        measurement_set,
                        properties,
                        get_observation(day_to_process.full_tar_name),
                        node_id
                    )

                if last_element is not None:
                    outputs.append(last_element)

            barrier_drop = self.create_barrier_app(node_id)
            carry_over_data.barrier_drop = barrier_drop

            for output in outputs:
                if output is not None:
                    barrier_drop.addInput(output)

        self.copy_logfiles_and_shutdown()

    def _split(self, last_element, frequency_pairs, measurement_set, properties, observation_name, node_id):
        if self._use_bash:
            casa_py_drop = self.create_casa_app(
                node_id,
                get_module_name(CasaMsTransform),
                'app_ms_transform',
                'ms_transform',
                casa_version=self._casa_version,
                min_frequency=frequency_pairs.bottom_frequency,
                max_frequency=frequency_pairs.top_frequency,
            )
        else:
            casa_py_drop = self.create_docker_app(
                node_id,
                get_module_name(DockerMsTransform),
                'app_ms_transform',
                CONTAINER_CHILES02,
                'ms_transform',
                min_frequency=frequency_pairs.bottom_frequency,
                max_frequency=frequency_pairs.top_frequency,
            )
        result = self.create_directory_container(node_id, 'dir_split')
        casa_py_drop.addInput(measurement_set)
        casa_py_drop.addInput(properties)
        if last_element is not None:
            casa_py_drop.addInput(last_element)

        casa_py_drop.addOutput(result)

        copy_to_s3 = self.create_app(
            node_id,
            get_module_name(CopyMsTransformToS3),
            'app_copy_mstransform_to_s3',
            min_frequency=frequency_pairs.bottom_frequency,
            max_frequency=frequency_pairs.top_frequency,
        )
        s3_drop_out = self.create_s3_drop(
            node_id,
            self._bucket_name,
            '{3}/{0}_{1}/{2}.tar'.format(
                frequency_pairs.bottom_frequency,
                frequency_pairs.top_frequency,
                observation_name,
                self._s3_split_name
            ),
            'aws-chiles02',
            oid='s3_out'
        )
        copy_to_s3.addInput(result)
        copy_to_s3.addOutput(s3_drop_out)

        return s3_drop_out

    def _setup_measurement_set(self, day_to_process, barrier_drop, add_output_s3, node_id):
        s3_drop = self.create_s3_drop(
            node_id,
            self._bucket_name,
            os.path.join('observation_data', day_to_process.full_tar_name),
            'aws-chiles02',
            's3_in')
        if len(add_output_s3) == 0:
            pass    # Do nothing
        else:
            for drop in add_output_s3:
                drop.addOutput(s3_drop)

        copy_from_s3 = self.create_app(node_id, get_module_name(CopyMsTransformFromS3), 'app_copy_mstransform_from_s3')
        measurement_set = self.create_directory_container(node_id, 'dir_in_ms', expire_after_use=False)

        if barrier_drop is not None:
            barrier_drop.addOutput(measurement_set)
        copy_from_s3.addInput(s3_drop)
        copy_from_s3.addOutput(measurement_set)

        if self._use_bash:
            drop_listobs = self.create_casa_app(
                node_id,
                get_module_name(CasaListobs),
                'app_listobs',
                'listobs',
                casa_version=self._casa_version,
            )
        else:
            drop_listobs = self.create_docker_app(
                node_id,
                get_module_name(DockerListobs),
                'app_listobs',
                CONTAINER_CHILES02,
                'listobs'
            )
        properties = self.create_json_drop(node_id)
        drop_listobs.addInput(measurement_set)
        drop_listobs.addOutput(properties)

        return measurement_set, properties, drop_listobs

    def _get_next_node(self, day_to_process):
        return self._map_day_to_node[day_to_process]

    def _build_node_map(self):
        # Set up the allocation directory
        allocation = {}
        for key, values in self._node_details.iteritems():
            allocation_dictionary = {}
            allocation[key] = allocation_dictionary
            for value in values:
                allocation_dictionary[value['ip_address']] = []

        for day_to_process in self._work_to_do.keys():
            if day_to_process.size <= 500 * SIZE_1GB:
                allocation_dictionary = allocation['i3.2xlarge']
                self._add_to_shortest_list(allocation_dictionary, day_to_process)
            else:
                allocation_dictionary = allocation['i3.4xlarge']
                self._add_to_shortest_list(allocation_dictionary, day_to_process)

        # Now balance the nodes a bit if needed
        if allocation.get('i3.2xlarge') is not None and allocation.get('i3.4xlarge') is not None:
            max_2xlarge = self._get_max(allocation['i3.2xlarge'])
            min_4xlarge = self._get_min(allocation['i3.4xlarge'])

            if max_2xlarge > min_4xlarge + 1:
                self._move_nodes(allocation['i3.2xlarge'], allocation['i3.4xlarge'])

        # Build the map
        self._map_day_to_node = {}
        for values in allocation.values():
            for key, days in values.iteritems():
                for day in days:
                    self._map_day_to_node[day] = key

    @property
    def map_day_to_node(self):
        return self._map_day_to_node

    @staticmethod
    def _add_to_shortest_list(allocation_dictionary, day_to_process):
        shortest_list = None
        for allocation_list in allocation_dictionary.values():
            if shortest_list is None:
                shortest_list = allocation_list
            elif len(allocation_list) < len(shortest_list):
                shortest_list = allocation_list

        shortest_list.append(day_to_process)

    @staticmethod
    def _get_biggest_list(i3_2xlists):
        biggest_list = None
        for allocation_list in i3_2xlists.values():
            if biggest_list is None:
                biggest_list = allocation_list
            elif len(allocation_list) > len(biggest_list):
                biggest_list = allocation_list

        return biggest_list

    @staticmethod
    def _get_max(allocation_dictionary):
        max_list_size = None
        for allocation_list in allocation_dictionary.values():
            if max_list_size is None:
                max_list_size = len(allocation_list)
            elif len(allocation_list) > max_list_size:
                max_list_size = len(allocation_list)

        return max_list_size

    @staticmethod
    def _get_min(allocation_dictionary):
        min_list_size = None
        for allocation_list in allocation_dictionary.values():
            if min_list_size is None:
                min_list_size = len(allocation_list)
            elif len(allocation_list) < min_list_size:
                min_list_size = len(allocation_list)

        return min_list_size

    def _move_nodes(self, i3_2xlists, i3_4xlists):
        max_2xlarge = None
        min_4xlarge = None

        while max_2xlarge is None or max_2xlarge > min_4xlarge + 1:
            biggest_list = self._get_biggest_list(i3_2xlists)
            day_to_process = biggest_list.pop()
            self._add_to_shortest_list(i3_4xlists, day_to_process)

            max_2xlarge = self._get_max(i3_2xlists)
            min_4xlarge = self._get_min(i3_4xlists)
