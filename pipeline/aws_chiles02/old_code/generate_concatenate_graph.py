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
import sys
from time import sleep

import os
from aws_chiles02.build_graph_concatenate import BuildGraphConcatenation
from configobj import ConfigObj

from aws_chiles02.common import TKINTER, get_aws_credentials, get_input_mode, get_session_id, get_uuid
from aws_chiles02.ec2_controller import EC2Controller
from aws_chiles02.generate_common import build_hosts, get_nodes_running, get_reported_running
from aws_chiles02.get_argument import GetArguments
from aws_chiles02.settings_file import AWS_AMI_ID, AWS_REGION, DIM_PORT
from aws_chiles02.user_data import get_data_island_manager_user_data, get_node_manager_user_data
from dfms.droputils import get_roots
from dfms.manager.client import DataIslandManagerClient

LOG = logging.getLogger(__name__)
PARALLEL_STREAMS = 16


def get_s3_clean_name(width, iterations, arcsec):
    return 'clean_{0}_{1}_{2}'.format(width, iterations, arcsec)


def create_and_generate(bucket_name, frequency_width, ami_id, spot_price, volume, add_shutdown, iterations):
    boto_data = get_aws_credentials('aws-chiles02')
    if boto_data is not None:
        uuid = get_uuid()
        ec2_data = EC2Controller(
            ami_id,
            [
                {
                    'number_instances': 1,
                    'instance_type': 'i2.4xlarge',
                    'spot_price': spot_price
                }
            ],
            get_node_manager_user_data(boto_data, uuid),
            AWS_REGION,
            tags=[
                {
                    'Key': 'Owner',
                    'Value': getpass.getuser(),
                },
                {
                    'Key': 'Name',
                    'Value': 'DALiuGE NM - Concatenate',
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
            1,
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
                    'spot_price': spot_price
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
                    'Value': 'DALiuGE DIM - Concatenate',
                },
                {
                    'Key': 'uuid',
                    'Value': uuid,
                },
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
            graph = BuildGraphConcatenation(
                bucket_name,
                volume,
                PARALLEL_STREAMS,
                reported_running,
                add_shutdown,
                frequency_width,
                iterations,
                '2arcsec',  # TODO: Pass as a parameter
                session_id,
                host)
            graph.build_graph()

            LOG.info('Connection to {0}:{1}'.format(host, DIM_PORT))
            client = DataIslandManagerClient(host, DIM_PORT)

            client.create_session(session_id)
            client.append_graph(session_id, graph.drop_list)
            client.deploy_session(session_id, get_roots(graph.drop_list))
    else:
        LOG.error('Unable to find the AWS credentials')


def use_and_generate(host, port, bucket_name, frequency_width, volume, add_shutdown, iterations):
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
            # Now build the graph
            session_id = get_session_id()
            graph = BuildGraphConcatenation(bucket_name, volume, PARALLEL_STREAMS, nodes_running, add_shutdown, frequency_width, iterations, session_id, host)
            graph.build_graph()

            LOG.info('Connection to {0}:{1}'.format(host, port))
            client = DataIslandManagerClient(host, port)

            client.create_session(session_id)
            client.append_graph(session_id, graph.drop_list)
            client.deploy_session(session_id, get_roots(graph.drop_list))

        else:
            LOG.warning('No nodes are running')


def command_json(args):
    node_details = {
        'number_instances': 1,
        'instance_type': 'm4.large',
        'spot_price': 0.99
    }

    graph = BuildGraphConcatenation(args.bucket, args.volume, args.parallel_streams, node_details, args.shutdown, args.width, args.iterations, 'session_id', '1.2.3.4')
    graph.build_graph()
    json_dumps = json.dumps(graph.drop_list, indent=2)
    LOG.info(json_dumps)
    with open("/tmp/json_split.txt", "w") as json_file:
        json_file.write(json_dumps)


def command_create(args):
    create_and_generate(
        args.bucket,
        args.width,
        args.ami,
        args.spot_price1,
        args.volume,
        args.shutdown,
        args.iterations,
    )


def command_use(args):
    use_and_generate(
        args.host,
        args.port,
        args.bucket,
        args.width,
        args.volume,
        args.shutdown,
        args.iterations,
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
        args.get('create_use', 'create or use', allowed=['create', 'use'], help_text='the use a network or create a network')
        args.get('bucket_name', 'Bucket name', help_text='the bucket to access', default='13b-266')
        args.get('volume', 'Volume', help_text='the directory on the host to bind to the Docker Apps')
        args.get('width', 'Frequency width', data_type=int, help_text='the frequency width', default=4)
        args.get('iterations', 'Clean iterations', data_type=int, help_text='the clean iterations', default=10)
        args.get('shutdown', 'Add the shutdown node', data_type=bool, help_text='add a shutdown drop', default=True)
        if config['create_use'] == 'create':
            args.get('ami', 'AMI Id', help_text='the AMI to use', default=AWS_AMI_ID)
            args.get('spot_price_i2_4xlarge', 'Spot Price for i2.4xlarge', help_text='the spot price')
        else:
            args.get('dim', 'Data Island Manager', help_text='the IP to the DataIsland Manager')

    # Write the arguments
    config.write()

    # Run the command
    if config['create_use'] == 'create':
        create_and_generate(
            config['bucket_name'],
            config['width'],
            config['ami'],
            config['spot_price_i2_4xlarge'],
            config['volume'],
            config['shutdown'],
            config['iterations'],
        )
    else:
        use_and_generate(
            config['dim'],
            DIM_PORT,
            config['bucket_name'],
            config['width'],
            config['volume'],
            config['shutdown'],
            config['iterations'],
        )


def parser_arguments(command_line=sys.argv[1:]):
    parser = argparse.ArgumentParser('Build the CONCATENATION physical graph for a day')

    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('bucket', help='the bucket to access')
    common_parser.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    common_parser.add_argument('--width', type=int, help='the frequency width', default=4)
    common_parser.add_argument('--shutdown', action="store_true", help='add a shutdown drop')
    common_parser.add_argument('--iterations', type=int, help='the number of iterations', default=10)
    common_parser.add_argument('-v', '--verbosity', action='count', default=0, help='increase output verbosity')

    subparsers = parser.add_subparsers()

    parser_json = subparsers.add_parser('json', parents=[common_parser], help='display the json')
    parser_json.add_argument('parallel_streams', type=int, help='the of parallel streams')
    parser_json.set_defaults(func=command_json)

    parser_create = subparsers.add_parser('create', parents=[common_parser], help='run and deploy')
    parser_create.add_argument('ami', help='the ami to use')
    parser_create.add_argument('spot_price', type=float, help='the spot price')
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
