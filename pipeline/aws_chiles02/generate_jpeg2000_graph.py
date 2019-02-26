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

from configobj import ConfigObj

from aws_chiles02.build_graph_jpeg2000 import BuildGraphJpeg2000
from aws_chiles02.common import TKINTER, get_aws_credentials, get_input_mode, get_session_id, get_uuid
from aws_chiles02.ec2_controller import EC2Controller
from aws_chiles02.generate_common import build_hosts, get_nodes_running, get_reported_running
from aws_chiles02.get_argument import GetArguments
from aws_chiles02.settings_file import AWS_AMI_ID, AWS_REGION, DIM_PORT, WAIT_TIMEOUT_NODE_MANAGER, WAIT_TIMEOUT_ISLAND_MANAGER
from aws_chiles02.user_data import get_data_island_manager_user_data, get_node_manager_user_data
from dlg.droputils import get_roots
from dlg.manager.client import DataIslandManagerClient

LOG = logging.getLogger(__name__)
PARALLEL_STREAMS = 8


def create_and_generate(**kwargs):
    boto_data = get_aws_credentials('aws-chiles02')
    if boto_data is not None:
        uuid = get_uuid()
        ami_id = kwargs['ami_id']
        spot_price = kwargs['spot_price']
        ec2_data = EC2Controller(
            ami_id,
            [
                {
                    'number_instances': 1,
                    'instance_type': 'i3.2xlarge',
                    'spot_price': spot_price
                }
            ],
            get_node_manager_user_data(boto_data, uuid, chiles=False, jpeg2000=True),
            AWS_REGION,
            tags=[
                {
                    'Key': 'Owner',
                    'Value': getpass.getuser(),
                },
                {
                    'Key': 'Name',
                    'Value': 'DALiuGE NM - JPEG2000',
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
            wait=WAIT_TIMEOUT_NODE_MANAGER
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
                    'Value': 'DALiuGE DIM - JPEG2000',
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
            wait=WAIT_TIMEOUT_ISLAND_MANAGER
        )

        if len(data_island_manager_running['m4.large']) == 1:
            # Now build the graph
            session_id = get_session_id()
            instance_details = data_island_manager_running['m4.large'][0]
            host = instance_details['ip_address']
            graph = BuildGraphJpeg2000(
                bucket_name=kwargs['bucket_name'],
                volume=kwargs['volume'],
                parallel_streams=PARALLEL_STREAMS,
                node_details=reported_running,
                shutdown=kwargs['add_shutdown'],
                fits_directory_name=kwargs['fits_directory_name'],
                jpeg2000_directory_name=kwargs['jpeg2000_directory_name'],
                session_id=session_id,
                dim_ip=host
            )
            graph.build_graph()

            LOG.info('Connection to {0}:{1}'.format(host, DIM_PORT))
            client = DataIslandManagerClient(host, DIM_PORT)

            client.create_session(session_id)
            client.append_graph(session_id, graph.drop_list)
            client.deploy_session(session_id, get_roots(graph.drop_list))
    else:
        LOG.error('Unable to find the AWS credentials')


def use_and_generate(**kwargs):
    boto_data = get_aws_credentials('aws-chiles02')
    if boto_data is not None:
        host = kwargs['host']
        port = kwargs['port']
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
            graph = BuildGraphJpeg2000(
                bucket_name=kwargs['bucket_name'],
                volume=kwargs['volume'],
                parallel_streams=PARALLEL_STREAMS,
                node_details=nodes_running,
                shutdown=kwargs['add_shutdown'],
                fits_directory_name=kwargs['fits_directory_name'],
                jpeg2000_directory_name=kwargs['jpeg2000_directory_name'],
                session_id=session_id,
                dim_ip=host
            )
            graph.build_graph()

            LOG.info('Connection to {0}:{1}'.format(host, port))
            client = DataIslandManagerClient(host, port)

            client.create_session(session_id)
            client.append_graph(session_id, graph.drop_list)
            client.deploy_session(session_id, get_roots(graph.drop_list))

        else:
            LOG.warning('No nodes are running')


def save_json(**kwargs):
    node_details = {
        'number_instances': 1,
        'instance_type': 'm4.large',
        'spot_price': 0.99
    }

    graph = BuildGraphJpeg2000(
        bucket_name=kwargs["bucket"],
        volume=kwargs["volume"],
        parallel_streams=PARALLEL_STREAMS,
        node_details=node_details,
        shutdown=kwargs["shutdown"],
        fits_directory_name='fits_dir',
        jpeg2000_directory_name='jpeg_dir',
        session_id='session_id',
        dim_ip='1.2.3.4',
    )
    graph.build_graph()
    json_dumps = json.dumps(graph.drop_list, indent=2)
    LOG.info(json_dumps)
    with open(kwargs.get('json_path', "/tmp/json_jpeg2000.txt"), "w") as json_file:
        json_file.write(json_dumps)


def command_json(args):
    save_json(bucket=args.bucket,
              volume=args.volume,
              shutdown=args.shutdown)


def command_create(args):
    create_and_generate(
        bucket_name=args.bucket,
        ami_id=args.ami,
        spot_price=args.spot_price1,
        volume=args.volume,
        add_shutdown=args.shutdown,
        fits_directory_name=args.fits_directory_name,
        jpeg2000_directory_name=args.jpeg2000_directory_name,
    )


def command_use(args):
    use_and_generate(
        host=args.host,
        port=args.port,
        bucket_name=args.bucket,
        volume=args.volume,
        add_shutdown=args.shutdown,
        fits_directory_name=args.fits_directory_name,
        jpeg2000_directory_name=args.jpeg2000_directory_name,
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
        args.get('create_use', 'Create or use', allowed=['create', 'use'], help_text='the use a network or create a network')
        args.get('bucket_name', 'Bucket name', help_text='the bucket to access', default='13b-266')
        args.get('volume', 'Volume', help_text='the directory on the host to bind to the Docker Apps')
        args.get('fits_directory_name', 'The directory name for fits files', help_text='the directory name for fits')
        args.get('jpeg2000_directory_name', 'The directory name for JPEG2000 files', help_text='the directory name for JPEG2000')
        args.get('shutdown', 'Add the shutdown node', data_type=bool, help_text='add a shutdown drop', default=True)

        if config['create_use'] == 'create':
            args.get('ami', 'AMI Id', help_text='the AMI to use', default=AWS_AMI_ID)
            args.get('spot_price_i3_2xlarge', 'Spot Price for i3.2xlarge', help_text='the spot price')
        else:
            args.get('dim', 'Data Island Manager', help_text='the IP to the DataIsland Manager')

    # Write the arguments
    config.write()

    # Run the command
    if config['create_use'] == 'create':
        create_and_generate(
            bucket_name=config['bucket_name'],
            ami_id=config['ami'],
            spot_price=config['spot_price_i3_2xlarge'],
            volume=config['volume'],
            add_shutdown=config['shutdown'],
            fits_directory_name=config['fits_directory_name'],
            jpeg2000_directory_name=config['jpeg2000_directory_name'],
        )
    else:
        use_and_generate(
            host=config['dim'],
            port=DIM_PORT,
            bucket_name=config['bucket_name'],
            volume=config['volume'],
            add_shutdown=config['shutdown'],
            fits_directory_name=config['fits_directory_name'],
            jpeg2000_directory_name=config['jpeg2000_directory_name'],
        )


def parser_arguments(command_line=sys.argv[1:]):
    parser = argparse.ArgumentParser('Build the JPEG2000 physical graph for a day')

    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('bucket', help='the bucket to access')
    common_parser.add_argument('arcsec', help='the number of arcsec', default='2')
    common_parser.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    common_parser.add_argument('--shutdown', action="store_true", help='add a shutdown drop')
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
    parser_use.add_argument('host', help='the host the DALiuGE is running on')
    parser_use.add_argument('--port', type=int, help='the port to bind to', default=DIM_PORT)
    parser_use.set_defaults(func=command_use)

    parser_interactive = subparsers.add_parser('interactive', help='prompt the user for parameters and then run')
    parser_interactive.set_defaults(func=command_interactive)

    args = parser.parse_args(command_line)
    return args


if __name__ == '__main__':
    # json 13b-266 /mnt/daliuge/dlg_root 8 -w 8 -s
    # interactive
    logging.basicConfig(
        level=logging.INFO,
        format='{asctime}:{levelname}:{name}:{message}',
        style='{',
    )
    arguments = parser_arguments()
    arguments.func(arguments)
