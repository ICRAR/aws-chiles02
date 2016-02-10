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
Build a dictionary for the execution graph
"""
import argparse
import json
import logging
import operator
import os

import boto3
from configobj import ConfigObj

from aws_chiles02.apps import DockerCopyFromS3, DockerCopyToS3, DockerMsTransform, DockerListobs
from aws_chiles02.common import get_oid, get_module_name, get_uid, get_session_id, get_observation, CONTAINER_JAVA_S3_COPY, CONTAINER_CHILES02, make_groups_of_frequencies, INPUT_MS_SUFFIX_TAR, \
    get_list_frequency_groups, FrequencyPair
from aws_chiles02.generate_common import AbstractBuildGraph
from dfms.drop import DirectoryContainer, dropdict, BarrierAppDROP
from dfms.manager.client import NodeManagerClient, SetEncoder

LOG = logging.getLogger(__name__)
_1GB = 1073741824


class MeasurementSetData:
    def __init__(self, full_tar_name, size):
        self.full_tar_name = full_tar_name
        self.size = size
        # Get rid of the '_calibrated_deepfield.ms.tar'
        self.short_name = full_tar_name[:-len(INPUT_MS_SUFFIX_TAR)]


class CarryOverData:
    def __init__(self):
        self.drop_listobs = None
        self.barrier_drop = None


class BuildGraph(AbstractBuildGraph):
    def __init__(self, command_line_args):
        super(BuildGraph, self).__init__(command_line_args)
        self._s3_split_name = 'split_{}'.format(self._width)
        self._node_name = None
        config_file_name = '{0}/graph.settings'.format(os.path.dirname(__file__))
        if os.path.exists(config_file_name):
            self._config = ConfigObj(config_file_name)
        else:
            self._config = None

    @staticmethod
    def get_details_for_measurement_set(splits_done, list_frequencies):
        frequency_groups = []
        if splits_done is None:
            frequency_groups.extend(list_frequencies)
        else:
            # Remove the groups we've processed
            for frequency_group in list_frequencies:
                if frequency_group.name not in splits_done:
                    frequency_groups.append(frequency_group)

        return frequency_groups

    def get_work_already_done(self):
        frequencies_per_day = {}
        for key in self._bucket.objects.filter(Prefix=self._s3_split_name):
            elements = key.key.split('/')

            day_key = elements[2]
            # Remove the .tar
            day_key = day_key[:-4]

            frequencies = frequencies_per_day.get(day_key)
            if frequencies is None:
                frequencies = []
                frequencies_per_day[day_key] = frequencies

            frequencies.append(elements[1])

        return frequencies_per_day

    def ignore_day(self, list_frequency_groups):
        if len(list_frequency_groups) >= 4:
            return False

        # Check if we have the first groups
        count_bottom = 0
        for bottom_frequency in range(940, 952, self._width):
            frequency_group = FrequencyPair(bottom_frequency, bottom_frequency + self._width)
            if frequency_group in list_frequency_groups:
                count_bottom += 1

        return len(list_frequency_groups) - count_bottom <= 0

    def build_graph(self):
        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)

        list_measurement_sets = []
        self._bucket = s3.Bucket(self._bucket_name)
        for key in self._bucket.objects.all():
            if key.key.endswith('_calibrated_deepfield.ms.tar'):
                obj = s3.Object(key.bucket_name, key.key)
                storage_class = obj.storage_class
                restore = obj.restore
                LOG.info('{0}, {1}, {2}'.format(key.key, storage_class, restore))

                ok_to_queue = True
                if 'GLACIER' == storage_class:
                    if restore is None or restore.startswith('ongoing-request="true"'):
                        ok_to_queue = False

                if ok_to_queue:
                    list_measurement_sets.append(MeasurementSetData(key.key, key.size))

        # Make sure the smallest is first
        sorted_list_measurement_sets = sorted(list_measurement_sets, key=operator.attrgetter('size'))

        # Get work we've already done
        list_frequencies = get_list_frequency_groups(self._width)
        work_already_done = self.get_work_already_done()

        map_carry_over_data = {}
        for node_id in range(0, self._nodes):
            map_carry_over_data[node_id] = CarryOverData()

        node_id = 0
        for day_to_process in sorted_list_measurement_sets:
            day_work_already_done = work_already_done.get(day_to_process.short_name)
            list_frequency_groups = self.get_details_for_measurement_set(day_work_already_done, list_frequencies)

            if self.ignore_day(list_frequency_groups):
                LOG.info('{0} has already been process.'.format(day_to_process.full_tar_name))
            else:
                self.setup_node_name(node_id)

                carry_over_data = map_carry_over_data[node_id]
                frequency_groups = make_groups_of_frequencies(list_frequency_groups, self._cores)

                add_output_s3 = []
                if carry_over_data.drop_listobs is not None:
                    add_output_s3.append(carry_over_data.drop_listobs)

                measurement_set, properties, drop_listobs = \
                    self.setup_measurement_set(
                            day_to_process,
                            carry_over_data.barrier_drop,
                            add_output_s3)

                carry_over_data.drop_listobs = drop_listobs

                outputs = []
                for group in frequency_groups:
                    last_element = None
                    for frequency_pairs in group:
                        last_element = self.split(
                                last_element,
                                frequency_pairs,
                                measurement_set,
                                properties,
                                get_observation(day_to_process.full_tar_name))

                    outputs.append(last_element)

                barrier_drop = dropdict({
                    "type": 'app',
                    "app": get_module_name(BarrierAppDROP),
                    "oid": get_oid('app_barrier'),
                    "uid": get_uid(),
                    "user": 'root',
                    "input_error_threshold": 100,
                    "node": self._node_name,
                })
                carry_over_data.barrier_drop = barrier_drop
                self.append(barrier_drop)

                for output in outputs:
                    barrier_drop.addInput(output)

                node_id += 1
                node_id = node_id % self._nodes

    def split(self, last_element, frequency_pairs, measurement_set, properties, observation_name):
        casa_py_drop = dropdict({
            "type": 'app',
            "app": get_module_name(DockerMsTransform),
            "oid": get_oid('app_ms_transform'),
            "uid": get_uid(),
            "image": CONTAINER_CHILES02,
            "command": 'mstransform',
            "min_frequency": frequency_pairs.bottom_frequency,
            "max_frequency": frequency_pairs.top_frequency,
            "user": 'root',
            "input_error_threshold": 100,
            "node": self._node_name,
            "n_tries": 2,
        })
        oid03 = get_oid('dir_split')
        result = dropdict({
            "type": 'container',
            "container": get_module_name(DirectoryContainer),
            "oid": oid03,
            "uid": get_uid(),
            "precious": False,
            "dirname": os.path.join(self._volume, oid03),
            "check_exists": False,
            "expireAfterUse": True,
            "node": self._node_name,
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
            "app": get_module_name(DockerCopyToS3),
            "oid": get_oid('app_copy_to_s3'),
            "uid": get_uid(),
            "image": CONTAINER_JAVA_S3_COPY,
            "command": 'copy_to_s3',
            "user": 'root',
            "min_frequency": frequency_pairs.bottom_frequency,
            "max_frequency": frequency_pairs.top_frequency,
            "additionalBindings": ['/home/ec2-user/.aws/credentials:/root/.aws/credentials'],
            "input_error_threshold": 100,
            "node": self._node_name,
            "n_tries": 2,
        })
        s3_drop_out = dropdict({
            "type": 'plain',
            "storage": 's3',
            "oid": get_oid('s3_out'),
            "uid": get_uid(),
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
            "node": self._node_name,
        })
        copy_to_s3.addInput(result)
        copy_to_s3.addOutput(s3_drop_out)
        self.append(copy_to_s3)
        self.append(s3_drop_out)
        return s3_drop_out

    def setup_measurement_set(self, day_to_process, barrier_drop, add_output_s3):
        s3_drop = dropdict({
            "type": 'plain',
            "storage": 's3',
            "oid": get_oid('s3_in'),
            "uid": get_uid(),
            "precious": False,
            "bucket": self._bucket_name,
            "key": day_to_process.full_tar_name,
            "profile_name": 'aws-chiles02',
            "node": self._node_name,
        })
        if len(add_output_s3) == 0:
            self._start_oids.append(s3_drop['uid'])
        else:
            for drop in add_output_s3:
                drop.addOutput(s3_drop)

        copy_from_s3 = dropdict({
            "type": 'app',
            "app": get_module_name(DockerCopyFromS3),
            "oid": get_oid('app_copy_from_s3'),
            "uid": get_uid(),
            "image": CONTAINER_JAVA_S3_COPY,
            "command": 'copy_from_s3',
            "additionalBindings": ['/home/ec2-user/.aws/credentials:/root/.aws/credentials'],
            "user": 'root',
            "input_error_threshold": 100,
            "node": self._node_name,
            "n_tries": 2,
        })
        oid01 = get_oid('dir_in_ms')
        measurement_set = dropdict({
            "type": 'container',
            "container": get_module_name(DirectoryContainer),
            "oid": oid01,
            "uid": get_uid(),
            "precious": False,
            "dirname": os.path.join(self._volume, oid01),
            "check_exists": False,
            "node": self._node_name,
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
            "oid": get_oid('app_listobs'),
            "uid": get_uid(),
            "image": CONTAINER_CHILES02,
            "command": 'listobs',
            "user": 'root',
            "input_error_threshold": 100,
            "node": self._node_name,
            "n_tries": 2,
        })
        oid02 = get_oid('json')
        properties = dropdict({
            "type": 'plain',
            "storage": 'json',
            "oid": oid02,
            "uid": get_uid(),
            "precious": False,
            "dirname": os.path.join(self._volume, oid02),
            "check_exists": False,
            "node": self._node_name,
        })
        drop_listobs.addInput(measurement_set)
        drop_listobs.addOutput(properties)
        self.append(drop_listobs)
        self.append(properties)
        return measurement_set, properties, drop_listobs

    def setup_node_name(self, node_id):
        if self._config is not None:
            self._node_name = self._config['node_{0}'.format(node_id)]
        else:
            self._node_name = 'localhost'


def command_json(args):
    graph = BuildGraph(args)
    graph.build_graph()
    json_dumps = json.dumps(graph.drop_list, indent=2, cls=SetEncoder)
    LOG.info(json_dumps)
    with open("/tmp/json_split.txt", "w") as json_file:
        json_file.write(json_dumps)


def command_run(args):
    graph = BuildGraph(args)
    graph.build_graph()

    client = NodeManagerClient(args.host, args.port)

    session_id = get_session_id()
    client.create_session(session_id)
    client.append_graph(session_id, graph.drop_list)
    client.deploy_session(session_id, graph.start_oids)


def parser_arguments():
    parser = argparse.ArgumentParser('Build the MSTRANSFORM physical graph for a day')

    subparsers = parser.add_subparsers()

    parser_json = subparsers.add_parser('json', help='display the json')
    parser_json.add_argument('bucket', help='the bucket to access')
    parser_json.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    parser_json.add_argument('cores', type=int, help='the number of cores')
    parser_json.add_argument('-w', '--width', type=int, help='the frequency width', default=4)
    parser_json.add_argument('-n', '--nodes', type=int, help='the number of nodes', default=1)
    parser_json.set_defaults(func=command_json)

    parser_run = subparsers.add_parser('run', help='run and deploy')
    parser_run.add_argument('bucket', help='the bucket to access')
    parser_run.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    parser_run.add_argument('cores', type=int, help='the number of cores')
    parser_run.add_argument('host', help='the host the dfms is running on')
    parser_run.add_argument('-p', '--port', type=int, help='the port to bind to', default=8000)
    parser_run.add_argument('-w', '--width', type=int, help='the frequency width', default=4)
    parser_run.add_argument('-n', '--nodes', type=int, help='the number of nodes', default=1)
    parser_run.set_defaults(func=command_run)

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    arguments = parser_arguments()
    arguments.func(arguments)
