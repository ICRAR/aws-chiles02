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
import getpass
import httplib
import json
import logging
import os
from time import sleep

import boto3
import sys
from configobj import ConfigObj

from aws_chiles02.build_graph_uvsub import BuildGraphUvsub
from aws_chiles02.common import get_session_id, get_argument, get_aws_credentials, get_uuid
from aws_chiles02.ec2_controller import EC2Controller
from aws_chiles02.generate_common import get_reported_running, build_hosts, get_nodes_running
from aws_chiles02.settings_file import AWS_REGION, AWS_AMI_ID, DIM_PORT
from aws_chiles02.user_data import get_node_manager_user_data, get_data_island_manager_user_data
from dfms.droputils import get_roots
from dfms.manager.client import DataIslandManagerClient

LOG = logging.getLogger(__name__)
PARALLEL_STREAMS = 8


class WorkToDo:
    def __init__(
            self,
            width,
            bucket_name,
            s3_uvsub_name,
            s3_split_name,
            min_frequency,
            max_frequency):
        self._width = width
        self._bucket_name = bucket_name
        self._s3_uvsub_name = s3_uvsub_name
        self._s3_split_name = s3_split_name
        self._min_frequency = min_frequency
        self._max_frequency = max_frequency

        self._work_already_done = None
        self._bucket = None
        self._work_to_do = []

    def calculate_work_to_do(self):
        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)

        uvsub_objects = []
        self._bucket = s3.Bucket(self._bucket_name)
        for key in self._bucket.objects.filter(Prefix='{0}'.format(self._s3_uvsub_name)):
            uvsub_objects.append(key.key)
            LOG.info('uvsub {0} found'.format(key.key))

        for key in self._bucket.objects.filter(Prefix='{0}'.format(self._s3_split_name)):
            LOG.info('split {0} found'.format(key.key))
            elements = key.key.split('/')
            if len(elements) == 3:
                expected_uvsub_name = 'uvsub_{0}/{1}/{2}'.format(
                    self._width,
                    elements[1],
                    elements[2],
                )
                if expected_uvsub_name not in uvsub_objects:
                    frequencies = elements[1].split('_')
                    # Use the min and max frequency
                    if self._min_frequency is not None and int(frequencies[1]) < self._min_frequency:
                        continue
                    if self._max_frequency is not None and int(frequencies[0]) > self._max_frequency:
                        continue

                    self._work_to_do.append(
                        [
                            elements[1],
                            elements[2],
                        ]
                    )

    @property
    def work_to_do(self):
        return self._work_to_do


def get_s3_split_name(width):
    return 'split_{0}'.format(width)


def get_s3_uvsub_name(width):
    return 'uvsub_{0}'.format(width)


def get_nodes_required(node_count, spot_price):
    nodes = [{
        'number_instances': node_count,
        'instance_type': 'i2.2xlarge',
        'spot_price': spot_price
    }]

    return nodes, node_count


def create_and_generate(
        bucket_name,
        frequency_width,
        w_projection_planes,
        ami_id,
        spot_price,
        volume,
        nodes,
        add_shutdown,
        min_frequency,
        max_frequency,
        scan_statistics):
    boto_data = get_aws_credentials('aws-chiles02')
    if boto_data is not None:
        work_to_do = WorkToDo(
            width=frequency_width,
            bucket_name=bucket_name,
            s3_uvsub_name=get_s3_uvsub_name(frequency_width),
            s3_split_name=get_s3_split_name(frequency_width),
            min_frequency=min_frequency,
            max_frequency=max_frequency)
        work_to_do.calculate_work_to_do()

        nodes_required, node_count = get_nodes_required(nodes, spot_price)

        if len(nodes_required) > 0:
            uuid = get_uuid()
            ec2_data = EC2Controller(
                ami_id,
                nodes_required,
                get_node_manager_user_data(boto_data, uuid, max_request_size=50),
                AWS_REGION,
                tags=[
                    {
                        'Key': 'Owner',
                        'Value': getpass.getuser(),
                    },
                    {
                        'Key': 'Name',
                        'Value': 'DALiuGE NM - Uvsub',
                    },
                    {
                        'Key': 'uuid',
                        'Value': uuid,
                    }
                ]
            )
            ec2_data.start_instances()

            reported_running = get_reported_running(
                uuid,
                node_count,
                wait=600
            )

            if len(reported_running) == 0:
                LOG.error('Nothing has reported ready')
            else:
                hosts = build_hosts(reported_running)

                # Create the Data Island Manager
                data_island_manager = EC2Controller(
                    ami_id,
                    [
                        {
                            'number_instances': 1,
                            'instance_type': 'm4.large',
                            'spot_price': spot_price
                        }
                    ],
                    get_data_island_manager_user_data(boto_data, hosts, uuid, max_request_size=50),
                    AWS_REGION,
                    tags=[
                        {
                            'Key': 'Owner',
                            'Value': getpass.getuser(),
                        },
                        {
                            'Key': 'Name',
                            'Value': 'DALiuGE DIM - Uvsub',
                        },
                        {
                            'Key': 'uuid',
                            'Value': uuid,
                        }
                    ]
                )
                data_island_manager.start_instances()
                data_island_manager_running = get_reported_running(
                        uuid,
                        1,
                        wait=600
                )

                if len(data_island_manager_running['m4.large']) == 1:
                    # Now build the graph
                    session_id = get_session_id()
                    instance_details = data_island_manager_running['m4.large'][0]
                    host = instance_details['ip_address']
                    graph = BuildGraphUvsub(
                        work_to_do=work_to_do.work_to_do,
                        bucket_name=bucket_name,
                        volume=volume,
                        parallel_streams=PARALLEL_STREAMS,
                        node_details=reported_running,
                        shutdown=add_shutdown,
                        scan_statistics=scan_statistics,
                        width=frequency_width,
                        w_projection_planes=w_projection_planes,
                        session_id=session_id,
                        dim_ip=host)
                    graph.build_graph()

                    LOG.info('Connection to {0}:{1}'.format(host, DIM_PORT))
                    client = DataIslandManagerClient(host, DIM_PORT)

                    client.create_session(session_id)
                    client.append_graph(session_id, graph.drop_list)
                    client.deploy_session(session_id, get_roots(graph.drop_list))
    else:
        LOG.error('Unable to find the AWS credentials')


def use_and_generate(
        host,
        port,
        bucket_name,
        frequency_width,
        w_projection_planes,
        volume,
        add_shutdown,
        min_frequency,
        max_frequency,
        scan_statistics):
    boto_data = get_aws_credentials('aws-chiles02')
    if boto_data is not None:
        connection = httplib.HTTPConnection(host, port)
        connection.request('GET', '/api', None, {})
        response = connection.getresponse()
        if response.status != httplib.OK:
            msg = 'Error while processing GET request for {0}:{1}/api (status {2}): {3}'.format(host, port, response.status, response.read())
            raise Exception(msg)

        json_data = response.read()
        message_details = json.loads(json_data)
        host_list = message_details['hosts']

        nodes_running = get_nodes_running(host_list)
        if len(nodes_running) > 0:
            work_to_do = WorkToDo(
                width=frequency_width,
                bucket_name=bucket_name,
                s3_uvsub_name=get_s3_uvsub_name(frequency_width),
                s3_split_name=get_s3_split_name(frequency_width),
                min_frequency=min_frequency,
                max_frequency=max_frequency)
            work_to_do.calculate_work_to_do()

            # Now build the graph
            session_id = get_session_id()
            graph = BuildGraphUvsub(
                work_to_do=work_to_do.work_to_do,
                bucket_name=bucket_name,
                volume=volume,
                parallel_streams=PARALLEL_STREAMS,
                node_details=nodes_running,
                shutdown=add_shutdown,
                scan_statistics=scan_statistics,
                width=frequency_width,
                w_projection_planes=w_projection_planes,
                session_id=session_id,
                dim_ip=host)
            graph.build_graph()

            LOG.info('Connection to {0}:{1}'.format(host, port))
            client = DataIslandManagerClient(host, port)

            client.create_session(session_id)
            client.append_graph(session_id, graph.drop_list)
            client.deploy_session(session_id, get_roots(graph.drop_list))

        else:
            LOG.warning('No nodes are running')


def generate_json(
        width,
        w_projection_planes,
        bucket,
        nodes,
        volume,
        shutdown,
        min_frequency,
        max_frequency,
        scan_statistics):
    work_to_do = WorkToDo(
        width=width,
        bucket_name=bucket,
        s3_uvsub_name=get_s3_uvsub_name(width),
        s3_split_name=get_s3_split_name(width),
        min_frequency=min_frequency,
        max_frequency=max_frequency)
    work_to_do.calculate_work_to_do()

    node_details = {
        'i2.2xlarge': [{'ip_address': 'node_i2_{0}'.format(i)} for i in range(0, nodes)],
    }
    graph = BuildGraphUvsub(
        work_to_do=work_to_do.work_to_do,
        bucket_name=bucket,
        volume=volume,
        parallel_streams=PARALLEL_STREAMS,
        node_details=node_details,
        shutdown=shutdown,
        scan_statistics=scan_statistics,
        width=width,
        w_projection_planes=w_projection_planes,
        session_id='session_id',
        dim_ip='1.2.3.4')
    graph.build_graph()
    json_dumps = json.dumps(graph.drop_list, indent=2)
    LOG.info(json_dumps)
    with open("/tmp/json_uvsub.txt", "w") as json_file:
        json_file.write(json_dumps)


def command_json(args):
    generate_json(
        width=args.width,
        w_projection_planes=args.w_projection_planes,
        bucket=args.bucket,
        nodes=args.nodes,
        volume=args.volume,
        shutdown=args.shutdown,
        min_frequency=args.min_frequency,
        max_frequency=args.max_frequency,
        scan_statistics=args.scan_statistics,
    )


def command_create(args):
    create_and_generate(
        bucket_name=args.bucket,
        frequency_width=args.width,
        w_projection_planes=args.w_projection_planes,
        ami_id=args.ami,
        spot_price=args.spot_price,
        volume=args.volume,
        nodes=args.nodes,
        add_shutdown=args.shutdown,
        min_frequency=args.min_frequency,
        max_frequency=args.max_frequency,
        scan_statistics=args.scan_statistics,
    )


def command_use(args):
    use_and_generate(
        host=args.host,
        port=args.port,
        bucket_name=args.bucket,
        frequency_width=args.width,
        w_projection_planes=args.w_projection_planes,
        volume=args.volume,
        add_shutdown=args.shutdown,
        min_frequency=args.min_frequency,
        max_frequency=args.max_frequency,
        scan_statistics=args.scan_statistics,
    )


def command_interactive(args):
    LOG.info(args)
    sleep(0.5)  # Allow the logging time to print
    path_dirname, filename = os.path.split(__file__)
    config_file_name = '{0}/aws-chiles02.settings'.format(path_dirname)
    if os.path.exists(config_file_name):
        config = ConfigObj(config_file_name)
    else:
        config = ConfigObj()
        config.filename = config_file_name

    get_argument(config, 'run_type', 'Create, use or json', allowed=['create', 'use', 'json'], help_text=' use a network or create a network or just produce the JSON')
    get_argument(config, 'bucket_name', 'Bucket name', help_text='the bucket to access', default='13b-266')
    get_argument(config, 'volume', 'Volume', help_text='the directory on the host to bind to the Docker Apps')
    get_argument(config, 'width', 'Frequency width', data_type=int, help_text='the frequency width', default=4)
    get_argument(config, 'w_projection_planes', 'W Projection planes', data_type=int, help_text='the number of w projections planes', default=24)
    get_argument(config, 'shutdown', 'Add the shutdown node', data_type=bool, help_text='add a shutdown drop', default=True)
    get_argument(config, 'scan_statistics', 'Generate scan statistics', data_type=bool, help_text='generate scan statistics', default=True)
    get_argument(config, 'frequency_range', 'Do you want to specify a range of frequencies', data_type=bool, help_text='Do you want to specify a range of frequencies', default=False)
    if config['frequency_range']:
        get_argument(config, 'min_frequency', 'The minimum frequency', data_type=int, help_text='the minimum frequency', default=944)
        get_argument(config, 'max_frequency', 'The maximum frequency', data_type=int, help_text='the maximum frequency', default=1420)

    if config['run_type'] == 'create':
        get_argument(config, 'ami', 'AMI Id', help_text='the AMI to use', default=AWS_AMI_ID)
        get_argument(config, 'spot_price', 'Spot Price for i2.2xlarge', help_text='the spot price')
        get_argument(config, 'nodes', 'Number of nodes', data_type=int, help_text='the number of nodes', default=1)
    elif config['run_type'] == 'use':
        get_argument(config, 'dim', 'Data Island Manager', help_text='the IP to the DataIsland Manager')
    else:
        get_argument(config, 'nodes', 'Number of nodes', data_type=int, help_text='the number of nodes', default=1)

    # Write the arguments
    config.write()

    # Run the command
    if config['run_type'] == 'create':
        create_and_generate(
            bucket_name=config['bucket_name'],
            frequency_width=config['width'],
            w_projection_planes=config['w_projection_planes'],
            ami_id=config['ami'],
            spot_price=config['spot_price'],
            volume=config['volume'],
            nodes=config['nodes'],
            add_shutdown=config['shutdown'],
            min_frequency=config['min_frequency'] if config['frequency_range'] else None,
            max_frequency=config['max_frequency'] if config['frequency_range'] else None,
            scan_statistics=config['scan_statistics'],
        )
    elif config['run_type'] == 'use':
        use_and_generate(
            host=config['dim'],
            port=DIM_PORT,
            bucket_name=config['bucket_name'],
            frequency_width=config['width'],
            w_projection_planes=config['w_projection_planes'],
            volume=config['volume'],
            add_shutdown=config['shutdown'],
            min_frequency=config['min_frequency'] if config['frequency_range'] else None,
            max_frequency=config['max_frequency'] if config['frequency_range'] else None,
            scan_statistics=config['scan_statistics'],
        )
    else:
        generate_json(
            width=config['width'],
            w_projection_planes=config['w_projection_planes'],
            bucket=config['bucket_name'],
            nodes=config['nodes'],
            volume=config['volume'],
            shutdown=config['shutdown'],
            min_frequency=config['min_frequency'] if config['frequency_range'] else None,
            max_frequency=config['max_frequency'] if config['frequency_range'] else None,
            scan_statistics=config['scan_statistics'],
        )


def parser_arguments(command_line=sys.argv[1:]):
    parser = argparse.ArgumentParser('Build the UVSUB physical graph for a day')

    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('bucket', help='the bucket to access')
    common_parser.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    common_parser.add_argument('--w_projection_planes', type=int, help='the number of w projections planes', default=24)
    common_parser.add_argument('--width', type=int, help='the frequency width', default=4)
    common_parser.add_argument('--shutdown', action="store_true", help='add a shutdown drop')
    common_parser.add_argument('--scan_statistics', action="store_true", help='generate scan statistics')
    common_parser.add_argument('-v', '--verbosity', action='count', default=0, help='increase output verbosity')

    subparsers = parser.add_subparsers()

    parser_json = subparsers.add_parser('json', parents=[common_parser], help='display the json')
    parser_json.add_argument('parallel_streams', type=int, help='the of parallel streams')
    parser_json.add_argument('--nodes', type=int, help='the number of nodes', default=1)
    parser_json.set_defaults(func=command_json)

    parser_create = subparsers.add_parser('create', parents=[common_parser], help='run and deploy')
    parser_create.add_argument('ami', help='the ami to use')
    parser_create.add_argument('spot_price', type=float, help='the spot price')
    parser_create.add_argument('--nodes', type=int, help='the number of nodes', default=1)
    parser_create.set_defaults(func=command_create)

    parser_use = subparsers.add_parser('use', parents=[common_parser], help='use what is running and deploy')
    parser_use.add_argument('host', help='the host the dfms is running on')
    parser_use.add_argument('--port', type=int, help='the port to bind to', default=DIM_PORT)
    parser_use.set_defaults(func=command_use)

    parser_interactive = subparsers.add_parser('interactive', help='prompt the user for parameters and then run')
    parser_interactive.set_defaults(func=command_interactive)

    args = parser.parse_args(command_line)
    return args


if __name__ == '__main__':
    # json 13b-266 /mnt/dfms/dfms_root 8 -w 8 -s
    # interactive
    logging.basicConfig(level=logging.INFO)
    arguments = parser_arguments()
    arguments.func(arguments)
