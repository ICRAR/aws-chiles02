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

from aws_chiles02.build_graph_uvsub import BuildGraphUvsub
from aws_chiles02.common import FrequencyPair, TKINTER, get_aws_credentials, get_input_mode, get_required_frequencies, get_session_id, get_uuid
from aws_chiles02.ec2_controller import EC2Controller
from aws_chiles02.generate_common import build_hosts, get_nodes_running, get_reported_running
from aws_chiles02.get_argument import GetArguments
from aws_chiles02.settings_file import AWS_AMI_ID, AWS_REGION, DIM_PORT, WAIT_TIMEOUT_NODE_MANAGER, WAIT_TIMEOUT_ISLAND_MANAGER
from aws_chiles02.user_data import get_data_island_manager_user_data, get_node_manager_user_data
from dlg.droputils import get_roots
from dlg.manager.client import DataIslandManagerClient

LOG = logging.getLogger(__name__)
PARALLEL_STREAMS = 6


class WorkToDo:
    def __init__(self, **keywords):
        self._width = keywords['width']
        self._bucket_name = keywords['bucket_name']
        self._s3_uvsub_name = keywords['s3_uvsub_name']
        self._s3_split_name = keywords['s3_split_name']
        self._frequency_range = get_required_frequencies(keywords['frequency_range'], self._width)

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
                expected_uvsub_name = '{0}/{1}/{2}'.format(
                    self._s3_uvsub_name,
                    elements[1],
                    elements[2],
                )
                if expected_uvsub_name not in uvsub_objects:
                    # Use the frequency
                    frequencies = elements[1].split('_')
                    frequency_pair = FrequencyPair(frequencies[0], frequencies[1])
                    if self._frequency_range is not None and frequency_pair not in self._frequency_range:
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


def get_nodes_required(node_count, spot_price):
    nodes = [{
        'number_instances': node_count,
        'instance_type': 'i3.2xlarge',
        'spot_price': spot_price
    }]

    return nodes, node_count


def create_and_generate(**keywords):
    boto_data = get_aws_credentials('aws-chiles02')
    if boto_data is not None:
        bucket_name = keywords['bucket_name']
        work_to_do = WorkToDo(
            width=keywords['frequency_width'],
            bucket_name=bucket_name,
            s3_uvsub_name=keywords['uvsub_directory_name'],
            s3_split_name=get_s3_split_name(keywords['frequency_width']),
            frequency_range=keywords['frequency_range'],
        )
        work_to_do.calculate_work_to_do()

        nodes = keywords['nodes']
        spot_price = keywords['spot_price']
        ami_id = keywords['ami_id']

        nodes_required, node_count = get_nodes_required(nodes, spot_price)

        if len(nodes_required) > 0:
            uuid = get_uuid()
            ec2_data = EC2Controller(
                ami_id,
                nodes_required,
                get_node_manager_user_data(
                    boto_data,
                    uuid,
                    max_request_size=50,
                    chiles=not keywords['use_bash'],
                    casa_version=keywords['casa_version'],
                ),
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
                wait=WAIT_TIMEOUT_NODE_MANAGER
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
                    wait=WAIT_TIMEOUT_ISLAND_MANAGER
                )

                if len(data_island_manager_running['m4.large']) == 1:
                    # Now build the graph
                    session_id = get_session_id()
                    instance_details = data_island_manager_running['m4.large'][0]
                    host = instance_details['ip_address']
                    graph = BuildGraphUvsub(
                        work_to_do=work_to_do.work_to_do,
                        bucket_name=bucket_name,
                        volume=keywords['volume'],
                        parallel_streams=PARALLEL_STREAMS,
                        node_details=reported_running,
                        shutdown=keywords['add_shutdown'],
                        scan_statistics=keywords['scan_statistics'],
                        width=keywords['frequency_width'],
                        w_projection_planes=keywords['w_projection_planes'],
                        number_taylor_terms=keywords['number_taylor_terms'],
                        uvsub_directory_name=keywords['uvsub_directory_name'],
                        session_id=session_id,
                        dim_ip=host,
                        run_note=keywords['run_note'],
                        use_bash=keywords['use_bash'],
                        split_directory=keywords['split_directory'],
                        casa_version=keywords['casa_version'],
                        produce_qa=keywords['produce_qa'],
                    )
                    graph.build_graph()

                    if keywords['dump_json']:
                        json_dumps = json.dumps(graph.drop_list, indent=2)
                        with open("/tmp/json_uvsub.txt", "w") as json_file:
                            json_file.write(json_dumps)

                    LOG.info('Connection to {0}:{1}'.format(host, DIM_PORT))
                    client = DataIslandManagerClient(host, DIM_PORT)

                    client.create_session(session_id)
                    client.append_graph(session_id, graph.drop_list)
                    client.deploy_session(session_id, get_roots(graph.drop_list))
    else:
        LOG.error('Unable to find the AWS credentials')


def use_and_generate(**keywords):
    boto_data = get_aws_credentials('aws-chiles02')
    if boto_data is not None:
        host = keywords['host']
        port = keywords['port']
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
            frequency_width = keywords['frequency_width']
            bucket_name = keywords['bucket_name']
            uvsub_directory_name = keywords['uvsub_directory_name']
            work_to_do = WorkToDo(
                width=frequency_width,
                bucket_name=bucket_name,
                s3_uvsub_name=uvsub_directory_name,
                s3_split_name=get_s3_split_name(frequency_width),
                frequency_range=keywords['frequency_range'],
            )
            work_to_do.calculate_work_to_do()

            # Now build the graph
            session_id = get_session_id()
            graph = BuildGraphUvsub(
                work_to_do=work_to_do.work_to_do,
                bucket_name=bucket_name,
                volume=keywords['volume'],
                parallel_streams=PARALLEL_STREAMS,
                node_details=nodes_running,
                shutdown=keywords['add_shutdown'],
                scan_statistics=keywords['scan_statistics'],
                width=frequency_width,
                w_projection_planes=keywords['w_projection_planes'],
                number_taylor_terms=keywords['number_taylor_terms'],
                uvsub_directory_name=uvsub_directory_name,
                session_id=session_id,
                dim_ip=host,
                run_note=keywords['run_note'],
                use_bash=keywords['use_bash'],
                split_directory=keywords['split_directory'],
                casa_version=keywords['casa_version'],
                produce_qa=keywords['produce_qa'],
            )
            graph.build_graph()

            if keywords['dump_json']:
                json_dumps = json.dumps(graph.drop_list, indent=2)
                with open("/tmp/json_uvsub.txt", "w") as json_file:
                    json_file.write(json_dumps)

            LOG.info('Connection to {0}:{1}'.format(host, port))
            client = DataIslandManagerClient(host, port)

            client.create_session(session_id)
            client.append_graph(session_id, graph.drop_list)
            client.deploy_session(session_id, get_roots(graph.drop_list))

        else:
            LOG.warning('No nodes are running')


def generate_json(**keywords):
    width = keywords['width']
    bucket = keywords['bucket']
    uvsub_directory_name = keywords['uvsub_directory_name']
    work_to_do = WorkToDo(
        width=width,
        bucket_name=bucket,
        s3_uvsub_name=uvsub_directory_name,
        s3_split_name=get_s3_split_name(width),
        frequency_range=keywords['frequency_range'],
    )
    work_to_do.calculate_work_to_do()

    node_details = {
        'i3.2xlarge': [{'ip_address': 'node_i2_{0}'.format(i)} for i in range(0, keywords['nodes'])],
    }
    graph = BuildGraphUvsub(
        work_to_do=work_to_do.work_to_do,
        bucket_name=bucket,
        volume=keywords['volume'],
        parallel_streams=PARALLEL_STREAMS,
        node_details=node_details,
        shutdown=keywords['shutdown'],
        scan_statistics=keywords['scan_statistics'],
        width=width,
        w_projection_planes=keywords['w_projection_planes'],
        number_taylor_terms=keywords['number_taylor_terms'],
        uvsub_directory_name=uvsub_directory_name,
        session_id='session_id',
        dim_ip='1.2.3.4',
        run_note=keywords['run_note'],
        use_bash=keywords['use_bash'],
        split_directory=keywords['split_directory'],
        casa_version=keywords['casa_version'],
        produce_qa=keywords['produce_qa'],
    )
    graph.build_graph()
    json_dumps = json.dumps(graph.drop_list, indent=2)
    LOG.info(json_dumps)
    with open("/tmp/json_uvsub.txt", "w") as json_file:
        json_file.write(json_dumps)


def command_json(args):
    generate_json(
        width=args.width,
        w_projection_planes=args.w_projection_planes,
        number_taylor_terms=args.number_taylor_terms,
        bucket=args.bucket,
        nodes=args.nodes,
        volume=args.volume,
        shutdown=args.shutdown,
        frequency_range=args.frequency_range,
        scan_statistics=args.scan_statistics,
        uvsub_directory_name=args.uvsub_directory_name,
        run_note=args.run_note,
    )


def command_create(args):
    create_and_generate(
        bucket_name=args.bucket,
        frequency_width=args.width,
        w_projection_planes=args.w_projection_planes,
        number_taylor_terms=args.number_taylor_terms,
        ami_id=args.ami,
        spot_price=args.spot_price,
        volume=args.volume,
        nodes=args.nodes,
        add_shutdown=args.shutdown,
        frequency_range=args.frequency_range,
        scan_statistics=args.scan_statistics,
        dump_json=False,
        uvsub_directory_name=args.uvsub_directory_name,
        run_note=args.run_note,
    )


def command_use(args):
    use_and_generate(
        host=args.host,
        port=args.port,
        bucket_name=args.bucket,
        frequency_width=args.width,
        w_projection_planes=args.w_projection_planes,
        number_taylor_terms=args.number_taylor_terms,
        volume=args.volume,
        add_shutdown=args.shutdown,
        frequency_range=args.frequency_range,
        scan_statistics=args.scan_statistics,
        dump_json=False,
        uvsub_directory_name=args.uvsub_directory_name,
        run_note=args.run_note,
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
        args.get('run_type', 'Create, use or json', allowed=['create', 'use', 'json'], help_text=' use a network or create a network or just produce the JSON')
        args.get('bucket_name', 'Bucket name', help_text='the bucket to access', default='13b-266')
        args.get('width', 'Frequency width', data_type=int, help_text='the frequency width', default=4)
        args.get('split_directory', 'Split Directory', help_text='where to store the split data', default='split_{}'.format(config['width']))
        args.get('w_projection_planes', 'W Projection planes', data_type=int, help_text='the number of w projections planes', default=24)
        args.get('number_taylor_terms', 'Number of Taylor terms', data_type=int, help_text='the number of taylor terms', default=2)
        args.get('shutdown', 'Add the shutdown node', data_type=bool, help_text='add a shutdown drop', default=True)
        args.get('scan_statistics', 'Generate scan statistics', data_type=bool, help_text='generate scan statistics', default=True)
        args.get('use_bash', 'Run CASA in Bash rather than Docker', data_type=bool, help_text='run casa in bash', default=True)
        args.get('produce_qa', 'Produce QA products (yes or no)', allowed=['yes', 'no'], help_text='should we produce the QA products')
        if config['use_bash']:
            args.get('casa_version', 'Which version of CASA', allowed=['4.7', '5.1'], help_text='the version of CASA', default='5.1')
        else:
            args.get('volume', 'Volume', help_text='the directory on the host to bind to the Docker Apps')
        args.get('uvsub_directory_name', 'The directory name for the uvsub output', help_text='the directory name for the uvsub output')
        args.get('frequency_range', 'Do you want to specify a range of frequencies', help_text='Do you want to specify a range of frequencies comma separated', default='')
        args.get('run_note_uvsub', 'A single line note about this run', help_text='A single line note about this run', default='No note')

        if config['run_type'] == 'create':
            args.get('ami', 'AMI Id', help_text='the AMI to use', default=AWS_AMI_ID)
            args.get('spot_price_i3_2xlarge', 'Spot Price for i3.2xlarge', help_text='the spot price')
            args.get('nodes', 'Number of nodes', data_type=int, help_text='the number of nodes', default=1)
            args.get('dump_json', 'Dump the json', data_type=bool, help_text='dump the json', default=False)
        elif config['run_type'] == 'use':
            args.get('dim', 'Data Island Manager', help_text='the IP to the DataIsland Manager')
            args.get('dump_json', 'Dump the json', data_type=bool, help_text='dump the json', default=False)
        else:
            args.get('nodes', 'Number of nodes', data_type=int, help_text='the number of nodes', default=1)

    # Write the arguments
    config.write()

    # Run the command
    if config['run_type'] == 'create':
        create_and_generate(
            bucket_name=config['bucket_name'],
            frequency_width=config['width'],
            w_projection_planes=config['w_projection_planes'],
            number_taylor_terms=config['number_taylor_terms'],
            ami_id=config['ami'],
            spot_price=config['spot_price_i3_2xlarge'],
            volume=config['volume'],
            nodes=config['nodes'],
            add_shutdown=config['shutdown'],
            frequency_range=config['frequency_range'],
            scan_statistics=config['scan_statistics'],
            uvsub_directory_name=config['uvsub_directory_name'],
            dump_json=config['dump_json'],
            run_note=config['run_note_uvsub'],
            use_bash=config['use_bash'],
            casa_version=config['casa_version'],
            split_directory=config['split_directory'],
            produce_qa=config['produce_qa'],
        )
    elif config['run_type'] == 'use':
        use_and_generate(
            host=config['dim'],
            port=DIM_PORT,
            bucket_name=config['bucket_name'],
            frequency_width=config['width'],
            w_projection_planes=config['w_projection_planes'],
            number_taylor_terms=config['number_taylor_terms'],
            volume=config['volume'],
            add_shutdown=config['shutdown'],
            frequency_range=config['frequency_range'],
            scan_statistics=config['scan_statistics'],
            uvsub_directory_name=config['uvsub_directory_name'],
            dump_json=config['dump_json'],
            run_note=config['run_note_uvsub'],
            use_bash=config['use_bash'],
            casa_version=config['casa_version'],
            split_directory=config['split_directory'],
            produce_qa=config['produce_qa'],
        )
    else:
        generate_json(
            width=config['width'],
            w_projection_planes=config['w_projection_planes'],
            number_taylor_terms=config['number_taylor_terms'],
            bucket=config['bucket_name'],
            nodes=config['nodes'],
            volume=config['volume'],
            shutdown=config['shutdown'],
            frequency_range=config['frequency_range'],
            scan_statistics=config['scan_statistics'],
            uvsub_directory_name=config['uvsub_directory_name'],
            run_note=config['run_note_uvsub'],
            use_bash=config['use_bash'],
            casa_version=config['casa_version'],
            split_directory=config['split_directory'],
            produce_qa=config['produce_qa'],
        )


def parser_arguments(command_line=sys.argv[1:]):
    parser = argparse.ArgumentParser('Build the UVSUB physical graph for a day')

    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('bucket', help='the bucket to access')
    common_parser.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    common_parser.add_argument('--w_projection_planes', type=int, help='the number of w projections planes', default=24)
    common_parser.add_argument('--number_taylor_terms', type=int, help='the number of taylor terms', default=2)
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
    parser_use.add_argument('host', help='the host the DALiuge is running on')
    parser_use.add_argument('--port', type=int, help='the port to bind to', default=DIM_PORT)
    parser_use.set_defaults(func=command_use)

    parser_interactive = subparsers.add_parser('interactive', help='prompt the user for parameters and then run')
    parser_interactive.set_defaults(func=command_interactive)

    args = parser.parse_args(command_line)
    return args


if __name__ == '__main__':
    # json 13b-266 /mnt/daliuge/dlg_root 8 -w 8 -s
    # interactive
    logging.basicConfig(level=logging.INFO)
    arguments = parser_arguments()
    arguments.func(arguments)
