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
import os

import boto3

from aws_chiles02.common import get_oid, get_module_name, get_uuid, get_session_id, CONTAINER_JAVA_S3_COPY, CONTAINER_CHILES02, make_groups_of_frequencies, get_list_frequency_groups
from aws_chiles02.apps import DockerCopyToS3, DockerClean, DockerCopyAllFromS3Folder
from aws_chiles02.generate_common import AbstractBuildGraph
from dfms.drop import DirectoryContainer, dropdict, BarrierAppDROP
from dfms.manager.client import NodeManagerClient, SetEncoder

LOG = logging.getLogger(__name__)


class BuildGraph(AbstractBuildGraph):
    def __init__(self, command_line_args):
        super(BuildGraph, self).__init__(command_line_args)

    def build_graph(self):
        oid01 = get_oid('dir')
        file_drop = ({
            "type": 'plain',
            "storage": 'file',
            "oid": oid01,
            "uid": get_uuid(),
            "precious": False,
            "dirname": os.path.join(self._volume, oid01),
            "check_exists": False
        })
        self.append(file_drop)
        self._start_oids.append(file_drop['uid'])

        list_frequencies = get_list_frequency_groups(self._width)
        work_to_do = self.get_work_to_do(list_frequencies)

        outputs = []
        # TODO - Filter on what we've done
        for group in make_groups_of_frequencies(work_to_do, self._cores):
            last_element = None
            for frequency_pairs in group:
                if last_element is not None:
                    add_output_s3 = [last_element]
                else:
                    add_output_s3 = [file_drop]

                last_element = self.clean_frequency_pair(add_output_s3, frequency_pairs)

            outputs.append(last_element)

        barrier_drop = dropdict({
            "type": 'app',
            "app": get_module_name(BarrierAppDROP),
            "oid": get_oid('app'),
            "uid": get_uuid(),
            "user": 'root'
        })
        self.append(barrier_drop)

        for output in outputs:
            barrier_drop.addInput(output)

    def clean_frequency_pair(self, add_output_s3, frequency_pairs):
        copy_from_s3 = dropdict({
            "type": 'app',
            "app": get_module_name(DockerCopyAllFromS3Folder),
            "oid": get_oid('app'),
            "uid": get_uuid(),
            "image": CONTAINER_JAVA_S3_COPY,
            "command": 'copy_from_all_from_s3_folder',
            "min_frequency": frequency_pairs[0],
            "max_frequency": frequency_pairs[1],
            "user": 'root'
        })
        oid02 = get_oid('dir')
        measurement_sets = dropdict({
            "type": 'container',
            "container": get_module_name(DirectoryContainer),
            "oid": oid02,
            "uid": get_uuid(),
            "precious": False,
            "dirname": os.path.join(self._volume, oid02),
            "check_exists": False
        })
        for drop in add_output_s3:
            copy_from_s3.addInput(drop)

        copy_from_s3.addOutput(measurement_sets)
        self.append(copy_from_s3)
        self.append(measurement_sets)
        casa_py_drop = dropdict({
            "type": 'app',
            "app": get_module_name(DockerClean),
            "oid": get_oid('app'),
            "uid": get_uuid(),
            "image": CONTAINER_CHILES02,
            "command": 'clean',
            "min_frequency": frequency_pairs[0],
            "max_frequency": frequency_pairs[1],
            "user": 'root'
        })
        oid03 = get_oid('dir')
        result = dropdict({
            "type": 'container',
            "container": get_module_name(DirectoryContainer),
            "oid": oid03,
            "uid": get_uuid(),
            "precious": False,
            "dirname": os.path.join(self._volume, oid03),
            "check_exists": False,
            "expireAfterUse": True
        })
        casa_py_drop.addInput(measurement_sets)
        casa_py_drop.addOutput(result)
        copy_to_s3 = dropdict({
            "type": 'app',
            "app": get_module_name(DockerCopyToS3),
            "oid": get_oid('app'),
            "uid": get_uuid(),
            "image": CONTAINER_JAVA_S3_COPY,
            "command": 'copy_to_s3',
            "user": 'root',
            "min_frequency": frequency_pairs[0],
            "max_frequency": frequency_pairs[1],
        })
        s3_drop_out = dropdict({
            "type": 'plain',
            "storage": 's3',
            "oid": get_oid('s3'),
            "uid": get_uuid(),
            "expireAfterUse": True,
            "precious": False,
            "bucket": self._bucket_name,
            "key": 'clean/{0}_{1}/{0}_{1}.tar'.format(
                    frequency_pairs[0],
                    frequency_pairs[1]
            ),
        })
        copy_to_s3.addInput(result)
        copy_to_s3.addOutput(s3_drop_out)
        self.append(copy_to_s3)
        self.append(s3_drop_out)
        end_of_last_element = s3_drop_out
        return end_of_last_element

    def get_work_to_do(self, list_frequencies):
        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)
        self._bucket = s3.Bucket(self._bucket_name)

        split_data = []
        for key in self._bucket.objects.filter(Prefix='split'):
            elements = key.key.split('/')
            split_data.append([elements[2][:-4], elements[1]])


def command_json(args):
    graph = BuildGraph(args)
    graph.build_graph()
    json_dumps = json.dumps(graph.drop_list, indent=2, cls=SetEncoder)
    LOG.info(json_dumps)
    with open("/tmp/json_clean.txt", "w") as json_file:
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
    parser = argparse.ArgumentParser('Build the CLEAN physical graph for a day')

    subparsers = parser.add_subparsers()

    parser_json = subparsers.add_parser('json', help='display the json')
    parser_json.add_argument('bucket', help='the bucket to access')
    parser_json.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    parser_json.add_argument('cores', type=int, help='the number of cores')
    parser_json.set_defaults(func=command_json)

    parser_run = subparsers.add_parser('run', help='run and deploy')
    parser_run.add_argument('bucket', help='the bucket to access')
    parser_run.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    parser_run.add_argument('cores', type=int, help='the number of cores')
    parser_run.add_argument('host', help='the host the dfms is running on')
    parser_run.add_argument('-p', '--port', type=int, help='the port to bind to', default=8000)
    parser_run.add_argument('-w', '--width', type=int, help='the frequency width', default=4)
    parser_run.set_defaults(func=command_run)

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    arguments = parser_arguments()
    arguments.func(arguments)
