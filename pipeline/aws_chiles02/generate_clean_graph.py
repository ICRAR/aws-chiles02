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

from aws_chiles02.build_graph_clean import BuildGraphClean
from aws_chiles02.common import get_session_id, get_list_frequency_groups, get_argument, get_aws_credentials, get_uuid, get_log_level
from aws_chiles02.ec2_controller import EC2Controller
from aws_chiles02.generate_common import get_reported_running, build_hosts, get_nodes_running
from aws_chiles02.settings_file import AWS_REGION, AWS_AMI_ID, DIM_PORT
from aws_chiles02.user_data import get_node_manager_user_data, get_data_island_manager_user_data
from dfms.droputils import get_roots
from dfms.manager.client import DataIslandManagerClient

LOG = logging.getLogger(__name__)
PARALLEL_STREAMS = 12


class WorkToDo:
    def __init__(self, width, bucket_name, s3_clean_name):
        self._width = width
        self._bucket_name = bucket_name
        self._s3_clean_name = s3_clean_name
        self._work_already_done = None
        self._bucket = None
        self._list_frequencies = None
        self._work_to_do = []

    def calculate_work_to_do(self):
        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)

        cleaned_objects = []
        self._bucket = s3.Bucket(self._bucket_name)
        for key in self._bucket.objects.filter(Prefix='{0}'.format(self._s3_clean_name)):
            cleaned_objects.append(key.key)
            LOG.info('{0} found'.format(key.key))

        # Get work we've already done
        self._list_frequencies = get_list_frequency_groups(self._width)
        for frequency_pair in self._list_frequencies:
            expected_tar_file = '{0}/cleaned_{1}_{2}.tar'.format(
                self._s3_clean_name,
                frequency_pair.bottom_frequency,
                frequency_pair.top_frequency,
            )
            if expected_tar_file not in cleaned_objects:
                self._work_to_do.append(frequency_pair)

    @property
    def work_to_do(self):
        return self._work_to_do


def get_s3_clean_name(width, iterations, arcsec):
    return 'clean_{0}_{1}_{2}'.format(width, iterations, arcsec)


def get_nodes_required(work_to_do, frequencies_per_node, spot_price):
    nodes = []
    node_count = max(len(work_to_do) / frequencies_per_node, 1)
    nodes.append({
        'number_instances': node_count,
        'instance_type': 'i2.4xlarge',
        'spot_price': spot_price
    })

    return nodes, node_count


def create_and_generate(
        bucket_name,
        frequency_width,
        ami_id,
        spot_price,
        volume,
        frequencies_per_node,
        add_shutdown,
        iterations,
        arcsec,
        w_projection_planes,
        robust,
        only_image,
        log_level):
    boto_data = get_aws_credentials('aws-chiles02')
    if boto_data is not None:
        work_to_do = WorkToDo(frequency_width, bucket_name, get_s3_clean_name(frequency_width, iterations, arcsec))
        work_to_do.calculate_work_to_do()

        nodes_required, node_count = get_nodes_required(work_to_do.work_to_do, frequencies_per_node, spot_price)

        if len(nodes_required) > 0:
            uuid = get_uuid()
            ec2_data = EC2Controller(
                ami_id,
                nodes_required,
                get_node_manager_user_data(boto_data, uuid, log_level=log_level),
                AWS_REGION,
                tags=[
                    {
                        'Key': 'Owner',
                        'Value': getpass.getuser(),
                    },
                    {
                        'Key': 'Name',
                        'Value': 'DALiuGE NM - Clean',
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
                    get_data_island_manager_user_data(boto_data, hosts, uuid, need_node_manager=True, log_level=log_level),
                    AWS_REGION,
                    tags=[
                        {
                            'Key': 'Owner',
                            'Value': getpass.getuser(),
                        },
                        {
                            'Key': 'Name',
                            'Value': 'DALiuGE DIM - Clean',
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
                    graph = BuildGraphClean(
                        work_to_do=work_to_do.work_to_do,
                        bucket_name=bucket_name,
                        volume=volume,
                        parallel_streams=PARALLEL_STREAMS,
                        node_details=reported_running,
                        shutdown=add_shutdown,
                        width=frequency_width,
                        iterations=iterations,
                        arcsec=arcsec,
                        w_projection_planes=w_projection_planes,
                        robust=robust,
                        only_image=only_image,
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
        volume,
        add_shutdown,
        iterations,
        arcsec,
        w_projection_planes,
        robust,
        only_image):
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
            work_to_do = WorkToDo(frequency_width, bucket_name, get_s3_clean_name(frequency_width, iterations, arcsec))
            work_to_do.calculate_work_to_do()

            # Now build the graph
            session_id = get_session_id()
            graph = BuildGraphClean(
                work_to_do=work_to_do.work_to_do,
                bucket_name=bucket_name,
                volume=volume,
                parallel_streams=PARALLEL_STREAMS,
                node_details=nodes_running,
                shutdown=add_shutdown,
                width=frequency_width,
                iterations=iterations,
                arcsec=arcsec,
                w_projection_planes=w_projection_planes,
                robust=robust,
                only_image=only_image,
                session_id=session_id,
                dim_ip=host)
            graph.build_graph()

            # TODO: Remove
            json_dumps = json.dumps(graph.drop_list, indent=2)
            LOG.info(json_dumps)
            with open("/tmp/json_clean.txt", "w") as json_file:
                json_file.write(json_dumps)
            # TODO: Remove

            LOG.info('Connection to {0}:{1}'.format(host, port))
            client = DataIslandManagerClient(host, port)

            client.create_session(session_id)
            client.append_graph(session_id, graph.drop_list)
            client.deploy_session(session_id, get_roots(graph.drop_list))

        else:
            LOG.warning('No nodes are running')


def generate_json(width, bucket, iterations, arcsec, nodes, volume, parallel_streams, shutdown, w_projection_planes, robust, only_image):
    work_to_do = WorkToDo(width, bucket, get_s3_clean_name(width, iterations, arcsec))
    work_to_do.calculate_work_to_do()

    node_details = {
        'i2.4xlarge': [{'ip_address': 'node_i2_{0}'.format(i)} for i in range(0, nodes)]
    }
    graph = BuildGraphClean(
        work_to_do=work_to_do.work_to_do,
        bucket_name=bucket,
        volume=volume,
        parallel_streams=parallel_streams,
        node_details=node_details,
        shutdown=shutdown,
        width=width,
        iterations=iterations,
        arcsec=arcsec + 'arcsec',
        w_projection_planes=w_projection_planes,
        robust=robust,
        only_image=only_image,
        session_id='session_id',
        dim_ip='1.2.3.4')
    graph.build_graph()
    json_dumps = json.dumps(graph.drop_list, indent=2)
    LOG.info(json_dumps)
    with open("/tmp/json_clean.txt", "w") as json_file:
        json_file.write(json_dumps)


def command_json(args):
    generate_json(
        width=args.width,
        bucket=args.bucket,
        iterations=args.iterations,
        arcsec=args.arcsec,
        nodes=args.nodes,
        volume=args.volume,
        parallel_streams=args.parallel_streams,
        shutdown=args.shutdown,
        w_projection_planes=args.w_projection_planes,
        robust=args.robust,
        only_image=args.only_image
    )


def command_create(args):
    log_level = get_log_level(args)
    create_and_generate(
        bucket_name=args.bucket,
        frequency_width=args.width,
        ami_id=args.ami,
        spot_price=args.spot_price1,
        volume=args.volume,
        frequencies_per_node=args.frequencies_per_node,
        add_shutdown=args.shutdown,
        iterations=args.iterations,
        arcsec=args.arcsec + 'arcsec',
        w_projection_planes=args.w_projection_planes,
        robust=args.robust,
        only_image=args.only_image,
        log_level=log_level,
    )


def command_use(args):
    use_and_generate(
        host=args.host,
        port=args.port,
        bucket_name=args.bucket,
        frequency_width=args.width,
        volume=args.volume,
        add_shutdown=args.shutdown,
        iterations=args.iterations,
        arcsec=args.arcsec + 'arcsec',
        w_projection_planes=args.w_projection_planes,
        robust=args.robust,
        only_image=args.only_image,
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

    get_argument(config, 'run_type', 'Create, use or json', allowed=['create', 'use', 'json'], help_text='the use a network or create a network')
    get_argument(config, 'bucket_name', 'Bucket name', help_text='the bucket to access', default='13b-266')
    get_argument(config, 'volume', 'Volume', help_text='the directory on the host to bind to the Docker Apps')
    get_argument(config, 'width', 'Frequency width', data_type=int, help_text='the frequency width', default=4)
    get_argument(config, 'iterations', 'Clean iterations', data_type=int, help_text='the clean iterations', default=1)
    get_argument(config, 'arcsec', 'How many arc seconds', help_text='the arc seconds', default='1.25')
    get_argument(config, 'w_projection_planes', 'W Projection planes', data_type=int, help_text='the number of w projections planes', default=24)
    get_argument(config, 'robust', 'Clean robust value', data_type=float, help_text='the robust value for clean', default=0.8)
    get_argument(config, 'only_image', 'Only the image to S3', data_type=bool, help_text='only copy the image to S3', default=False)
    get_argument(config, 'shutdown', 'Add the shutdown node', data_type=bool, help_text='add a shutdown drop', default=True)
    if config['run_type'] == 'create':
        get_argument(config, 'ami', 'AMI Id', help_text='the AMI to use', default=AWS_AMI_ID)
        get_argument(config, 'spot_price_i2_4xlarge', 'Spot Price for i2.4xlarge', help_text='the spot price')
        get_argument(config, 'frequencies_per_node', 'Number of frequencies per node', data_type=int, help_text='the number of frequencies per node', default=1)
        get_argument(config, 'log_level', 'Log level', allowed=['v', 'vv', 'vvv'], help_text='the log level', default='vvv')
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
            ami_id=config['ami'],
            spot_price=config['spot_price_i2_4xlarge'],
            volume=config['volume'],
            frequencies_per_node=config['frequencies_per_node'],
            add_shutdown=config['shutdown'],
            iterations=config['iterations'],
            arcsec=config['arcsec'] + 'arcsec',
            w_projection_planes=config['w_projection_planes'],
            robust=config['robust'],
            only_image=config['only_image'],
            log_level=config['log_level'],
        )
    elif config['run_type'] == 'use':
        use_and_generate(
            host=config['dim'],
            port=DIM_PORT,
            bucket_name=config['bucket_name'],
            frequency_width=config['width'],
            volume=config['volume'],
            add_shutdown=config['shutdown'],
            iterations=config['iterations'],
            arcsec=config['arcsec'] + 'arcsec',
            w_projection_planes=config['w_projection_planes'],
            robust=config['robust'],
            only_image=config['only_image'],
        )
    else:
        generate_json(
            width=config['width'],
            bucket=config['bucket_name'],
            iterations=config['iterations'],
            arcsec=config['arcsec'] + 'arcsec',
            nodes=config['nodes'],
            volume=config['volume'],
            parallel_streams=PARALLEL_STREAMS,
            shutdown=config['shutdown'],
            w_projection_planes=config['w_projection_planes'],
            robust=config['robust'],
            only_image=config['only_image']
        )


def parser_arguments(command_line=sys.argv[1:]):
    parser = argparse.ArgumentParser('Build the CLEAN physical graph for a day')

    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('bucket', help='the bucket to access')
    common_parser.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    common_parser.add_argument('arcsec', help='the number of arcsec', default='1.25')
    common_parser.add_argument('--only_image', action='store_true', help='store only the image to S3', )
    common_parser.add_argument('--width', type=int, help='the frequency width', default=4)
    common_parser.add_argument('--shutdown', action='store_true', help='add a shutdown drop')
    common_parser.add_argument('--iterations', type=int, help='the number of iterations', default=10)
    common_parser.add_argument('-v', '--verbosity', action='count', default=0, help='increase output verbosity')
    common_parser.add_argument('--w_projection_planes', type=int, help='the number of w projections planes', default=24)
    common_parser.add_argument('--robust', type=float, help='the robust value for clean', default=0.8)

    subparsers = parser.add_subparsers()

    parser_json = subparsers.add_parser('json', parents=[common_parser], help='display the json')
    parser_json.add_argument('parallel_streams', type=int, help='the of parallel streams')
    parser_json.add_argument('--frequencies_per_node', type=int, help='the number of frequencies per node', default=1)
    parser_json.set_defaults(func=command_json)

    parser_create = subparsers.add_parser('create', parents=[common_parser], help='run and deploy')
    parser_create.add_argument('ami', help='the ami to use')
    parser_create.add_argument('spot_price', type=float, help='the spot price')
    parser_create.add_argument('--frequencies_per_node', type=int, help='the number of frequencies per node', default=1)
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
