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
import os
import operator

from aws_chiles02.apps import DockerMsTransform, DockerCopyMsTransformToS3, DockerCopyMsTransformFromS3, DockerListobs
from aws_chiles02.common import get_module_name, get_observation, make_groups_of_frequencies
from aws_chiles02.build_graph_common import AbstractBuildGraph
from aws_chiles02.settings_file import CONTAINER_CHILES02, CONTAINER_JAVA_S3_COPY, SIZE_1GB
from dfms.drop import dropdict, BarrierAppDROP, DirectoryContainer


class CarryOverDataMsTransform:
    def __init__(self):
        self.drop_listobs = None
        self.barrier_drop = None


class BuildGraphMsTransform(AbstractBuildGraph):
    def __init__(self, work_to_do, bucket_name, volume, parallel_streams, node_details, shutdown, width):
        super(BuildGraphMsTransform, self).__init__(bucket_name, shutdown, node_details, volume)
        self._work_to_do = work_to_do
        self._parallel_streams = parallel_streams
        self._s3_split_name = 'split_{0}'.format(width)

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

            barrier_drop = dropdict({
                "type": 'app',
                "app": get_module_name(BarrierAppDROP),
                "oid": self.get_oid('app_barrier'),
                "uid": self.get_uuid(),
                "user": 'root',
                "input_error_threshold": 100,
                "node": node_id,
            })
            carry_over_data.barrier_drop = barrier_drop
            self.append(barrier_drop)

            for output in outputs:
                barrier_drop.addInput(output)

        # Should we add a shutdown drop
        if self._shutdown:
            self._add_shutdown()

    def _split(self, last_element, frequency_pairs, measurement_set, properties, observation_name, node_id):
        casa_py_drop = dropdict({
            "type": 'app',
            "app": get_module_name(DockerMsTransform),
            "oid": self.get_oid('app_ms_transform'),
            "uid": self.get_uuid(),
            "image": CONTAINER_CHILES02,
            "command": 'mstransform',
            "min_frequency": frequency_pairs.bottom_frequency,
            "max_frequency": frequency_pairs.top_frequency,
            "user": 'root',
            "input_error_threshold": 100,
            "node": node_id,
            "n_tries": 2,
        })
        oid03 = self.get_oid('dir_split')
        result = dropdict({
            "type": 'container',
            "container": get_module_name(DirectoryContainer),
            "oid": oid03,
            "uid": self.get_uuid(),
            "precious": False,
            "dirname": os.path.join(self._volume, oid03),
            "check_exists": False,
            "expireAfterUse": True,
            "node": node_id,
        })
        casa_py_drop.addInput(measurement_set)
        casa_py_drop.addInput(properties)
        if last_element is not None:
            casa_py_drop.addInput(last_element)

        casa_py_drop.addOutput(result)
        self.append(casa_py_drop)
        self.append(result)
        copy_to_s3 = dropdict({
            "type": 'app',
            "app": get_module_name(DockerCopyMsTransformToS3),
            "oid": self.get_oid('app_copy_mstransform_to_s3'),
            "uid": self.get_uuid(),
            "image": CONTAINER_JAVA_S3_COPY,
            "command": 'copy_to_s3',
            "user": 'root',
            "min_frequency": frequency_pairs.bottom_frequency,
            "max_frequency": frequency_pairs.top_frequency,
            "additionalBindings": ['/home/ec2-user/.aws/credentials:/root/.aws/credentials'],
            "input_error_threshold": 100,
            "node": node_id,
            "n_tries": 2,
        })
        s3_drop_out = dropdict({
            "type": 'plain',
            "storage": 's3',
            "oid": self.get_oid('s3_out'),
            "uid": self.get_uuid(),
            "expireAfterUse": True,
            "precious": False,
            "bucket": self._bucket_name,
            "key": '{3}/{0}_{1}/{2}.tar'.format(
                    frequency_pairs.bottom_frequency,
                    frequency_pairs.top_frequency,
                    observation_name,
                    self._s3_split_name
            ),
            "profile_name": 'aws-chiles02',
            "node": node_id,
        })
        copy_to_s3.addInput(result)
        copy_to_s3.addOutput(s3_drop_out)
        self.append(copy_to_s3)
        self.append(s3_drop_out)
        return s3_drop_out

    def _setup_measurement_set(self, day_to_process, barrier_drop, add_output_s3, node_id):
        s3_drop = dropdict({
            "type": 'plain',
            "storage": 's3',
            "oid": self.get_oid('s3_in'),
            "uid": self.get_uuid(),
            "precious": False,
            "bucket": self._bucket_name,
            "key": day_to_process.full_tar_name,
            "profile_name": 'aws-chiles02',
            "node": node_id,
        })
        if len(add_output_s3) == 0:
            self._start_oids.append(s3_drop['uid'])
        else:
            for drop in add_output_s3:
                drop.addOutput(s3_drop)

        copy_from_s3 = dropdict({
            "type": 'app',
            "app": get_module_name(DockerCopyMsTransformFromS3),
            "oid": self.get_oid('app_copy_from_s3'),
            "uid": self.get_uuid(),
            "image": CONTAINER_JAVA_S3_COPY,
            "command": 'copy_from_s3',
            "additionalBindings": ['/home/ec2-user/.aws/credentials:/root/.aws/credentials'],
            "user": 'root',
            "input_error_threshold": 100,
            "node": node_id,
            "n_tries": 2,
        })
        oid01 = self.get_oid('dir_in_ms')
        measurement_set = dropdict({
            "type": 'container',
            "container": get_module_name(DirectoryContainer),
            "oid": oid01,
            "uid": self.get_uuid(),
            "precious": False,
            "dirname": os.path.join(self._volume, oid01),
            "check_exists": False,
            "node": node_id,
        })

        if barrier_drop is not None:
            barrier_drop.addOutput(measurement_set)
        copy_from_s3.addInput(s3_drop)
        copy_from_s3.addOutput(measurement_set)
        self.append(s3_drop)
        self.append(copy_from_s3)
        self.append(measurement_set)
        drop_listobs = dropdict({
            "type": 'app',
            "app": get_module_name(DockerListobs),
            "oid": self.get_oid('app_listobs'),
            "uid": self.get_uuid(),
            "image": CONTAINER_CHILES02,
            "command": 'listobs',
            "user": 'root',
            "input_error_threshold": 100,
            "node": node_id,
            "n_tries": 2,
        })
        oid02 = self.get_oid('json')
        properties = dropdict({
            "type": 'plain',
            "storage": 'json',
            "oid": oid02,
            "uid": self.get_uuid(),
            "precious": False,
            "dirname": os.path.join(self._volume, oid02),
            "check_exists": False,
            "node": node_id,
        })
        drop_listobs.addInput(measurement_set)
        drop_listobs.addOutput(properties)
        self.append(drop_listobs)
        self.append(properties)
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
                allocation_dictionary = allocation['i2.2xlarge']
                self._add_to_shortest_list(allocation_dictionary, day_to_process)
            else:
                allocation_dictionary = allocation['i2.4xlarge']
                self._add_to_shortest_list(allocation_dictionary, day_to_process)

        # Now balance the nodes a bit if needed
        if allocation.get('i2.2xlarge') is not None and allocation.get('i2.4xlarge') is not None:
            max_2xlarge = self._get_max(allocation['i2.2xlarge'])
            min_4xlarge = self._get_min(allocation['i2.4xlarge'])

            if max_2xlarge > min_4xlarge + 1:
                self._move_nodes(allocation['i2.2xlarge'], allocation['i2.4xlarge'])

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
    def _get_biggest_list(i2_2xlists):
        biggest_list = None
        for allocation_list in i2_2xlists.values():
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

    def _move_nodes(self, i2_2xlists, i2_4xlists):
        max_2xlarge = None
        min_4xlarge = None

        while max_2xlarge is None or max_2xlarge > min_4xlarge + 1:
            biggest_list = self._get_biggest_list(i2_2xlists)
            day_to_process = biggest_list.pop()
            self._add_to_shortest_list(i2_4xlists, day_to_process)

            max_2xlarge = self._get_max(i2_2xlists)
            min_4xlarge = self._get_min(i2_4xlists)
