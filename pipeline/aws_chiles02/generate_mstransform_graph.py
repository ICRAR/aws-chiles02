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
import sys
from time import sleep

import boto3
from configobj import ConfigObj

from aws_chiles02.build_graph_mstransform import BuildGraphMsTransform
from aws_chiles02.common import FrequencyPair, MeasurementSetData, TKINTER, get_aws_credentials, get_input_mode, get_list_frequency_groups, get_session_id, get_uuid
from aws_chiles02.ec2_controller import EC2Controller
from aws_chiles02.generate_common import build_hosts, get_nodes_running, get_reported_running
from aws_chiles02.get_argument import GetArguments
from aws_chiles02.settings_file import AWS_AMI_ID, AWS_REGION, DIM_PORT, SIZE_1GB
from aws_chiles02.user_data import get_data_island_manager_user_data, get_node_manager_user_data
from dfms.droputils import get_roots
from dfms.manager.client import DataIslandManagerClient

LOG = logging.getLogger(__name__)


class WorkToDo:
    def __init__(self, width, bucket_name, s3_split_name):
        self._width = width
        self._bucket_name = bucket_name
        self._s3_split_name = s3_split_name
        self._work_already_done = None
        self._bucket = None
        self._list_frequencies = None
        self._work_to_do = {}

    def calculate_work_to_do(self):
        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)

        list_measurement_sets = []
        self._bucket = s3.Bucket(self._bucket_name)
        for key in self._bucket.objects.filter(Prefix='observation_data'):
            if key.key.endswith('_calibrated_deepfield.ms.tar'):
                LOG.info('Found {0}'.format(key.key))

                elements = key.key.split('/')
                list_measurement_sets.append(MeasurementSetData(elements[1], key.size))

        # Get work we've already done
        self._list_frequencies = get_list_frequency_groups(self._width)
        self._work_already_done = self._get_work_already_done()

        for day_to_process in list_measurement_sets:
            day_work_already_done = self._work_already_done.get(day_to_process.short_name)
            list_frequency_groups = self._get_details_for_measurement_set(day_work_already_done)

            if self._ignore_day(list_frequency_groups):
                LOG.info('{0} has already been process.'.format(day_to_process.full_tar_name))
            else:
                self._work_to_do[day_to_process] = list_frequency_groups

    def _get_work_already_done(self):
        frequencies_per_day = {}
        for key in self._bucket.objects.filter(Prefix=self._s3_split_name):
            elements = key.key.split('/')

            if len(elements) > 2:
                day_key = elements[2]
                # Remove the .tar
                day_key = day_key[:-4]

                frequencies = frequencies_per_day.get(day_key)
                if frequencies is None:
                    frequencies = []
                    frequencies_per_day[day_key] = frequencies

                frequencies.append(elements[1])

        return frequencies_per_day

    def _get_details_for_measurement_set(self, splits_done):
        frequency_groups = []
        if splits_done is None:
            frequency_groups.extend(self._list_frequencies)
        else:
            # Remove the groups we've processed
            for frequency_group in self._list_frequencies:
                if frequency_group.underscore_name not in splits_done:
                    frequency_groups.append(frequency_group)

        return frequency_groups

    def _ignore_day(self, list_frequency_groups):
        # Check if we have the first groups
        count_bottom = 0
        for bottom_frequency in range(940, 956, self._width):
            frequency_group = FrequencyPair(bottom_frequency, bottom_frequency + self._width)
            if frequency_group in list_frequency_groups:
                count_bottom += 1

        return len(list_frequency_groups) - count_bottom <= 0

    @property
    def work_to_do(self):
        return self._work_to_do


def get_s3_split_name(width):
    return 'split_{0}'.format(width)


def get_nodes_required(days, days_per_node, spot_price1, spot_price2):
    nodes = []
    counts = [0, 0]
    for day in days:
        if day.size <= 500 * SIZE_1GB:
            counts[0] += 1
        else:
            counts[1] += 1

    node_count = 0
    if counts[1] > 0:
        count = max(counts[1] / days_per_node, 1)
        node_count += count
        nodes.append({
            'number_instances': count,
            'instance_type': 'i3.4xlarge',
            'spot_price': spot_price2
        })
    if counts[0] > 0:
        count = max(counts[0] / days_per_node, 1)
        node_count += count
        nodes.append({
            'number_instances': count,
            'instance_type': 'i3.2xlarge',
            'spot_price': spot_price1
        })

    return nodes, node_count


def create_and_generate(bucket_name, frequency_width, ami_id, spot_price1, spot_price2, volume, days_per_node, add_shutdown):
    boto_data = get_aws_credentials('aws-chiles02')
    if boto_data is not None:
        work_to_do = WorkToDo(
            width=frequency_width,
            bucket_name=bucket_name,
            s3_split_name=get_s3_split_name(frequency_width))
        work_to_do.calculate_work_to_do()

        days = work_to_do.work_to_do.keys()
        nodes_required, node_count = get_nodes_required(
            days=days,
            days_per_node=days_per_node,
            spot_price1=spot_price1,
            spot_price2=spot_price2)

        if len(nodes_required) > 0:
            uuid = get_uuid()
            ec2_data = EC2Controller(
                ami_id,
                nodes_required,
                get_node_manager_user_data(boto_data, uuid),
                AWS_REGION,
                tags=[
                    {
                        'Key': 'Owner',
                        'Value': getpass.getuser(),
                    },
                    {
                        'Key': 'Name',
                        'Value': 'DALiuGE NM - MsTransform',
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
                wait=900
            )
            hosts = build_hosts(reported_running)

            # Create the Data Island Manager
            data_island_manager = EC2Controller(
                ami_id,
                [
                    {
                        'number_instances': 1,
                        'instance_type': 'm4.large',
                        'spot_price': spot_price1
                    }
                ],
                get_data_island_manager_user_data(boto_data, hosts, uuid),
                AWS_REGION,
                tags=[
                    {
                        'Key': 'Owner',
                        'Value': getpass.getuser(),
                    },
                    {
                        'Key': 'Name',
                        'Value': 'DALiuGE DIM - MsTransform',
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
                wait=900
            )

            if len(data_island_manager_running['m4.large']) == 1:
                # Now build the graph
                session_id = get_session_id()
                instance_details = data_island_manager_running['m4.large'][0]
                host = instance_details['ip_address']
                graph = BuildGraphMsTransform(
                    work_to_do=work_to_do.work_to_do,
                    bucket_name=bucket_name,
                    volume=volume,
                    parallel_streams=7,
                    node_details=reported_running,
                    shutdown=add_shutdown,
                    width=frequency_width,
                    session_id=session_id,
                    dim_ip=hosts,
                )
                graph.build_graph()
                graph.tag_all_app_drops({
                    "session_id": session_id,
                })

                LOG.info('Connection to {0}:{1}'.format(host, DIM_PORT))
                client = DataIslandManagerClient(host, DIM_PORT)

                client.create_session(session_id)
                client.append_graph(session_id, graph.drop_list)
                client.deploy_session(session_id, get_roots(graph.drop_list))
    else:
        LOG.error('Unable to find the AWS credentials')


def use_and_generate(host, port, bucket_name, frequency_width, volume, add_shutdown):
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
                s3_split_name=get_s3_split_name(frequency_width))
            work_to_do.calculate_work_to_do()

            # Now build the graph
            session_id = get_session_id()
            graph = BuildGraphMsTransform(
                work_to_do=work_to_do.work_to_do,
                bucket_name=bucket_name,
                volume=volume,
                parallel_streams=7,
                node_details=nodes_running,
                shutdown=add_shutdown,
                width=frequency_width,
                session_id=session_id,
                dim_ip=host,
            )
            graph.build_graph()

            LOG.info('Connection to {0}:{1}'.format(host, port))
            client = DataIslandManagerClient(host, port)

            client.create_session(session_id)
            client.append_graph(session_id, graph.drop_list)
            client.deploy_session(session_id, get_roots(graph.drop_list))

        else:
            LOG.warning('No nodes are running')


def build_json(bucket, width, volume, nodes, parallel_streams, add_shutdown):
    work_to_do = WorkToDo(width, bucket, get_s3_split_name(width))
    work_to_do.calculate_work_to_do()

    node_details = {
        'i3.2xlarge': [{'ip_address': 'node_i2_{0}'.format(i)} for i in range(0, nodes)],
        'i3.4xlarge': [{'ip_address': 'node_i4_{0}'.format(i)} for i in range(0, nodes)],
    }
    graph = BuildGraphMsTransform(
        work_to_do=work_to_do.work_to_do,
        bucket_name=bucket,
        volume=volume,
        parallel_streams=parallel_streams,
        node_details=node_details,
        shutdown=add_shutdown,
        width=width,
        session_id='json_test',
        dim_ip='1.2.3.4'
    )
    graph.build_graph()
    json_dumps = json.dumps(graph.drop_list, indent=2)
    LOG.info(json_dumps)
    with open("/tmp/json_mstransform.txt", "w") as json_file:
        json_file.write(json_dumps)


def command_create(args):
    create_and_generate(
        bucket_name=args.bucket,
        frequency_width=args.width,
        ami_id=args.ami,
        spot_price1=args.spot_price1,
        spot_price2=args.spot_price2,
        volume=args.volume,
        days_per_node=args.days_per_node,
        add_shutdown=args.shutdown,
    )


def command_use(args):
    use_and_generate(
        host=args.host,
        port=args.port,
        bucket_name=args.bucket,
        frequency_width=args.width,
        volume=args.volume,
        add_shutdown=args.shutdown,
    )


def command_json(args):
    build_json(
        bucket=args.bucket,
        width=args.width,
        volume=args.volume,
        nodes=args.nodes,
        parallel_streams=args.parallel_streams,
        add_shutdown=args.shutdown,
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

    mode = get_input_mode()
    if mode == TKINTER and False:
        # TODO:
        pass
    else:
        args = GetArguments(config=config, mode=mode)
        args.get('create_use_json', 'Create, use or json', allowed=['create', 'use', 'json'], help_text='the use a network or create a network')
        args.get('bucket_name', 'Bucket name', help_text='the bucket to access', default='13b-266')
        args.get('volume', 'Volume', help_text='the directory on the host to bind to the Docker Apps')
        args.get('width', 'Frequency width', data_type=int, help_text='the frequency width', default=4)
        args.get('shutdown', 'Add the shutdown node', data_type=bool, help_text='add a shutdown drop', default=True)
        if config['create_use_json'] == 'create':
            args.get('ami', 'AMI Id', help_text='the AMI to use', default=AWS_AMI_ID)
            args.get('spot_price_i3.2xlarge', 'Spot Price for i3.2xlarge', help_text='the spot price')
            args.get('spot_price_i3_4xlarge', 'Spot Price for i3.4xlarge', help_text='the spot price')
            args.get('days_per_node', 'Number of days per node', data_type=int, help_text='the number of days per node', default=1)
        elif config['create_use_json'] == 'use':
            args.get('dim', 'Data Island Manager', help_text='the IP to the DataIsland Manager')
        else:
            args.get('nodes', 'Number nodes', data_type=int, help_text='the number of nodes', default=8)
            args.get('parallel_streams', 'Parallel streams', data_type=int, help_text='the number of parallel streams', default=4)

    # Run the command
    if config['create_use_json'] == 'create':
        command_line = 'create {0} {1} {2} {3} {4} {5} {6} {7}'.format(
            config['bucket_name'],
            config['volume'],
            config['ami'],
            config['spot_price_i3.2xlarge'],
            config['spot_price_i3_4xlarge'],
            '--days_per_node ' + config['days_per_node'],
            '--width ' + config['width'],
            '--shutdown' if config['shutdown'] else ''
        )
        create_and_generate(
            bucket_name=config['bucket_name'],
            frequency_width=config['width'],
            ami_id=config['ami'],
            spot_price1=config['spot_price_i3.2xlarge'],
            spot_price2=config['spot_price_i3_4xlarge'],
            volume=config['volume'],
            days_per_node=config['days_per_node'],
            add_shutdown=config['shutdown'],
        )
    elif config['create_use_json'] == 'use':
        command_line = 'use {0} {1} {2} {3} {4} {5}'.format(
            config['bucket_name'],
            config['volume'],
            config['dim'],
            '--port ' + DIM_PORT,
            '--width ' + config['width'],
            '--shutdown' if config['shutdown'] else ''
        )
        use_and_generate(
            host=config['dim'],
            port=DIM_PORT,
            bucket_name=config['bucket_name'],
            frequency_width=config['width'],
            volume=config['volume'],
            add_shutdown=config['shutdown'],
        )
    else:
        command_line = 'json'.format(

        )
        build_json(
            bucket=config['bucket_name'],
            width=config['width'],
            volume=config['volume'],
            nodes=config['nodes'],
            parallel_streams=config['parallel_streams'],
            add_shutdown=config['shutdown'],
        )
    return parser_arguments(command_line)


def parser_arguments(command_line=sys.argv[1:]):
    parser = argparse.ArgumentParser('Build the MSTRANSFORM physical graph for a day')

    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('bucket', help='the bucket to access')
    common_parser.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    common_parser.add_argument('-v', '--verbosity', action='count', default=0, help='increase output verbosity')
    common_parser.add_argument('--width', type=int, help='the frequency width', default=4)
    common_parser.add_argument('--shutdown', action="store_true", help='add a shutdown drop')
    common_parser.add_argument('--output', help='the output name', default=None)

    subparsers = parser.add_subparsers()

    parser_create = subparsers.add_parser('create', parents=[common_parser], help='run and deploy')
    parser_create.add_argument('ami', help='the ami to use')
    parser_create.add_argument('spot_price1', type=float, help='the spot price for the i3.2xlarge instances')
    parser_create.add_argument('spot_price2', type=float, help='the spot price for the i3.4xlarge instances')
    parser_create.add_argument('--days_per_node', type=int, help='the number of days per node', default=1)
    parser_create.set_defaults(func=command_create)

    parser_use = subparsers.add_parser('use', parents=[common_parser], help='use what is running and deploy')
    parser_use.add_argument('host', help='the host the dfms is running on')
    parser_use.add_argument('--port', type=int, help='the port to bind to', default=DIM_PORT)
    parser_use.set_defaults(func=command_use)

    parser_json = subparsers.add_parser('json', parents=[common_parser], help='use what is running and deploy')
    parser_json.set_defaults(func=command_json)

    parser_interactive = subparsers.add_parser('interactive', help='prompt the user for parameters and then run')
    parser_interactive.set_defaults(func=command_interactive)

    args = parser.parse_args(command_line)
    return args


if __name__ == '__main__':
    # interactive
    logging.basicConfig(level=logging.INFO)
    arguments = parser_arguments()
    arguments.func(arguments)
