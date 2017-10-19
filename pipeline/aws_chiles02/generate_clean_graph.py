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

import boto3
import os
from configobj import ConfigObj

from aws_chiles02.build_graph_clean import BuildGraphClean
from aws_chiles02.common import TKINTER, get_aws_credentials, get_input_mode, get_list_frequency_groups, get_log_level, get_required_frequencies, get_session_id, get_uuid
from aws_chiles02.ec2_controller import EC2Controller
from aws_chiles02.generate_common import build_hosts, get_nodes_running, get_reported_running
from aws_chiles02.get_argument import GetArguments
from aws_chiles02.settings_file import AWS_AMI_ID, AWS_REGION, DIM_PORT, WAIT_TIMEOUT_NODE_MANAGER, WAIT_TIMEOUT_ISLAND_MANAGER
from aws_chiles02.user_data import get_data_island_manager_user_data, get_node_manager_user_data
from dlg.droputils import get_roots
from dlg.manager.client import DataIslandManagerClient

LOG = logging.getLogger(__name__)
PARALLEL_STREAMS = 12


class WorkToDo:
    def __init__(self, **keywords):
        width = keywords['frequency_width']
        self._width = width
        self._bucket_name = keywords['bucket_name']
        self._s3_clean_name = keywords['clean_directory_name']
        self._s3_uvsub_name = keywords['uvsub_directory_name']
        self._frequency_range = get_required_frequencies(keywords['frequency_range'], width)
        self._work_to_do = []

    def calculate_work_to_do(self):
        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)

        cleaned_objects = []
        bucket = s3.Bucket(self._bucket_name)
        for key in bucket.objects.filter(Prefix='{0}'.format(self._s3_clean_name)):
            cleaned_objects.append(key.key)
            LOG.info('{0} found'.format(key.key))

        uvsub_frequencies = []
        for key in bucket.objects.filter(Prefix='{0}'.format(self._s3_uvsub_name)):
            elements = key.key.split('/')
            if len(elements) == 3:
                if elements[1] not in uvsub_frequencies:
                    uvsub_frequencies.append(elements[1])
                    LOG.info('{0} found'.format(key.key))

        # Get work we've already done
        list_frequencies = get_list_frequency_groups(self._width)
        for frequency_pair in list_frequencies:
            # Use the frequency
            if self._frequency_range is not None and frequency_pair not in self._frequency_range:
                continue

            expected_tar_file = '{0}/cleaned_{1}_{2}.tar'.format(
                self._s3_clean_name,
                frequency_pair.bottom_frequency,
                frequency_pair.top_frequency,
            )
            uvsub_frequency = '{0}_{1}'.format(frequency_pair.bottom_frequency, frequency_pair.top_frequency)
            if expected_tar_file not in cleaned_objects and uvsub_frequency in uvsub_frequencies:
                self._work_to_do.append(frequency_pair)

    @property
    def work_to_do(self):
        return self._work_to_do


def get_nodes_required(work_to_do, frequencies_per_node, spot_price):
    nodes = []
    node_count = max(len(work_to_do) / frequencies_per_node, 1)
    nodes.append({
        'number_instances': node_count,
        'instance_type': 'i3.4xlarge',
        'spot_price': spot_price
    })

    return nodes, node_count


def create_and_generate(**keywords):
    boto_data = get_aws_credentials('aws-chiles02')
    if boto_data is not None:
        frequency_width = keywords['frequency_width']
        bucket_name = keywords['bucket_name']
        clean_directory_name = keywords['clean_directory_name']
        uvsub_directory_name = keywords['uvsub_directory_name']
        work_to_do = WorkToDo(
            bucket_name=bucket_name,
            frequency_width=frequency_width,
            frequency_range=keywords['frequency_range'],
            clean_directory_name=clean_directory_name,
            uvsub_directory_name=uvsub_directory_name,
        )
        work_to_do.calculate_work_to_do()

        spot_price = keywords['spot_price']
        nodes_required, node_count = get_nodes_required(
            work_to_do.work_to_do,
            keywords['frequencies_per_node'],
            spot_price)

        if len(nodes_required) > 0:
            uuid = get_uuid()
            ami_id = keywords['ami_id']
            log_level = keywords['log_level']
            clean_tclean = keywords['clean_tclean']
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
                        'Value': 'DALiuGE NM - Clean' if clean_tclean == 'clean' else 'DALiuGE NM - Tclean',
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
                    get_data_island_manager_user_data(boto_data, hosts, uuid, log_level=log_level),
                    AWS_REGION,
                    tags=[
                        {
                            'Key': 'Owner',
                            'Value': getpass.getuser(),
                        },
                        {
                            'Key': 'Name',
                            'Value': 'DALiuGE DIM - Clean' if clean_tclean == 'clean' else 'DALiuGE DIM - Tclean',
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
                    graph = BuildGraphClean(
                        work_to_do=work_to_do.work_to_do,
                        bucket_name=bucket_name,
                        volume=keywords['volume'],
                        parallel_streams=PARALLEL_STREAMS,
                        node_details=reported_running,
                        shutdown=keywords['add_shutdown'],
                        width=frequency_width,
                        iterations=keywords['iterations'],
                        arcsec=keywords['arcsec'],
                        w_projection_planes=keywords['w_projection_planes'],
                        robust=keywords['robust'],
                        image_size=keywords['image_size'],
                        clean_channel_average=keywords['clean_channel_average'],
                        clean_directory_name=clean_directory_name,
                        only_image=keywords['only_image'],
                        session_id=session_id,
                        dim_ip=host,
                        produce_qa=keywords['produce_qa'],
                        uvsub_directory_name=uvsub_directory_name,
                        fits_directory_name=keywords['fits_directory_name'],
                        clean_tclean=clean_tclean,
                        run_note=keywords['run_note'],
                        use_bash=keywords['use_bash'],
                    )
                    graph.build_graph()

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
            clean_directory_name = keywords['clean_directory_name']
            uvsub_directory_name = keywords['uvsub_directory_name']
            work_to_do = WorkToDo(
                frequency_width=frequency_width,
                bucket_name=bucket_name,
                clean_directory_name=clean_directory_name,
                frequency_range=keywords['frequency_range'],
                uvsub_directory_name=uvsub_directory_name,
            )
            work_to_do.calculate_work_to_do()

            # Now build the graph
            session_id = get_session_id()
            graph = BuildGraphClean(
                work_to_do=work_to_do.work_to_do,
                bucket_name=bucket_name,
                volume=keywords['volume'],
                parallel_streams=PARALLEL_STREAMS,
                node_details=nodes_running,
                shutdown=keywords['add_shutdown'],
                width=frequency_width,
                iterations=keywords['iterations'],
                arcsec=keywords['arcsec'],
                w_projection_planes=keywords['w_projection_planes'],
                robust=keywords['robust'],
                image_size=keywords['image_size'],
                clean_channel_average=keywords['clean_channel_average'],
                clean_directory_name=clean_directory_name,
                only_image=keywords['only_image'],
                session_id=session_id,
                dim_ip=host,
                produce_qa=keywords['produce_qa'],
                uvsub_directory_name=uvsub_directory_name,
                fits_directory_name=keywords['fits_directory_name'],
                clean_tclean=keywords['clean_tclean'],
                run_note=keywords['run_note'],
                use_bash=keywords['use_bash'],
            )
            graph.build_graph()

            LOG.info('Connection to {0}:{1}'.format(host, port))
            client = DataIslandManagerClient(host, port)

            client.create_session(session_id)
            client.append_graph(session_id, graph.drop_list)
            client.deploy_session(session_id, get_roots(graph.drop_list))

        else:
            LOG.warning('No nodes are running')


def generate_json(**keywords):
    frequency_width = keywords['frequency_width']
    bucket_name = keywords['bucket_name']
    clean_directory_name = keywords['clean_directory_name']
    uvsub_directory_name = keywords['uvsub_directory_name']
    work_to_do = WorkToDo(
        frequency_width=frequency_width,
        bucket_name=bucket_name,
        clean_directory_name=clean_directory_name,
        frequency_range=keywords['frequency_range'],
        uvsub_directory_name=uvsub_directory_name,
    )
    work_to_do.calculate_work_to_do()

    node_details = {
        'i3.4xlarge': [{'ip_address': 'node_i2_{0}'.format(i)} for i in range(0, keywords['nodes'])]
    }
    graph = BuildGraphClean(
        work_to_do=work_to_do.work_to_do,
        bucket_name=bucket_name,
        volume=keywords['volume'],
        parallel_streams=keywords['parallel_streams'],
        node_details=node_details,
        shutdown=keywords['shutdown'],
        width=frequency_width,
        iterations=keywords['iterations'],
        arcsec=keywords['arcsec'] + 'arcsec',
        w_projection_planes=keywords['w_projection_planes'],
        robust=keywords['robust'],
        image_size=keywords['image_size'],
        clean_channel_average=keywords['clean_channel_average'],
        clean_directory_name=clean_directory_name,
        uvsub_directory_name=uvsub_directory_name,
        fits_directory_name=keywords['fits_directory_name'],
        only_image=keywords['only_image'],
        session_id='session_id',
        dim_ip='1.2.3.4',
        produce_qa=keywords['produce_qa'],
        clean_tclean=keywords['clean_tclean'],
        run_note=keywords['run_note'],
        use_bash=keywords['use_bash'],
    )
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
        image_size=args.image_size,
        clean_channel_average=args.clean_channel_average,
        frequency_range=args.frequency_range,
        clean_directory_name=args.clean_directory_name,
        only_image=args.only_image,
        produce_qa=args.produce_qa,
        uvsub_directory_name=args.uvsub_directory_name,
        fits_directory_name=args.fits_directory_name,
        clean_tclean=args.clean_tclean,
        run_note=args.run_note_clean,
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
        image_size=args.image_size,
        clean_channel_average=args.clean_channel_average,
        frequency_range=args.frequency_range,
        clean_directory_name=args.clean_directory_name,
        only_image=args.only_image,
        produce_qa=args.produce_qa,
        log_level=log_level,
        uvsub_directory_name=args.uvsub_directory_name,
        fits_directory_name=args.fits_directory_name,
        clean_tclean=args.clean_tclean,
        run_note=args.run_note_clean,
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
        image_size=args.image_size,
        clean_channel_average=args.clean_channel_average,
        frequency_range=args.frequency_range,
        clean_directory_name=args.clean_directory_name,
        produce_qa=args.produce_qa,
        only_image=args.only_image,
        uvsub_directory_name=args.uvsub_directory_name,
        fits_directory_name=args.fits_directory_name,
        clean_tclean=args.clean_tclean,
        run_note=args.run_note_clean,
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
        args.get('run_type', 'Create, use or json', allowed=['create', 'use', 'json'], help_text='the use a network or create a network')
        args.get('bucket_name', 'Bucket name', help_text='the bucket to access', default='13b-266')
        args.get('volume', 'Volume', help_text='the directory on the host to bind to the Docker Apps')
        args.get('width', 'Frequency width', data_type=int, help_text='the frequency width', default=4)
        args.get('iterations', 'Clean iterations', data_type=int, help_text='the clean iterations', default=1)
        args.get('arcsec', 'How many arc seconds', help_text='the arc seconds', default='2')
        args.get('w_projection_planes', 'W Projection planes', data_type=int, help_text='the number of w projections planes', default=24)
        args.get('robust', 'Clean robust value', data_type=float, help_text='the robust value for clean', default=0.8)
        args.get('image_size', 'The image size', data_type=int, help_text='the image size for clean', default=4096)
        args.get('clean_channel_average', 'The number of input channels to average', data_type=int, help_text='the number of input channels to average', default=1)
        args.get('only_image', 'Only the image to S3', data_type=bool, help_text='only copy the image to S3', default=False)
        args.get('shutdown', 'Add the shutdown node', data_type=bool, help_text='add a shutdown drop', default=True)
        args.get('uvsub_directory_name', 'The directory name for the uvsub output', help_text='the directory name for the uvsub output')
        args.get('clean_directory_name', 'The directory name for clean', help_text='the directory name for clean')
        args.get('fits_directory_name', 'The directory name for fits files', help_text='the directory name for fits')
        args.get('produce_qa', 'Produce QA products (yes or no)', allowed=['yes', 'no'], help_text='should we produce the QA products')
        args.get('clean_tclean', 'Clean or Tclean', allowed=['clean', 'tclean'], help_text='use clean or tclean', default='clean')
        args.get('use_bash', 'Run CASA in Bash rather than Docker', data_type=bool, help_text='run casa in bash', default=True)
        args.get('frequency_range', 'Do you want to specify a range of frequencies', help_text='Do you want to specify a range of frequencies comma separated', default='')
        args.get('run_note_clean', 'A single line note about this run', help_text='A single line note about this run', default='No note')

        if config['run_type'] == 'create':
            args.get('ami', 'AMI Id', help_text='the AMI to use', default=AWS_AMI_ID)
            args.get('spot_price_i3_4xlarge', 'Spot Price for i3.4xlarge', help_text='the spot price')
            args.get('frequencies_per_node', 'Number of frequencies per node', data_type=int, help_text='the number of frequencies per node', default=1)
            args.get('log_level', 'Log level', allowed=['v', 'vv', 'vvv'], help_text='the log level', default='vvv')
        elif config['run_type'] == 'use':
            args.get('dim', 'Data Island Manager', help_text='the IP to the DataIsland Manager')
        else:
            args.get('nodes', 'Number of nodes', data_type=int, help_text='the number of nodes', default=1)

    # Write the arguments
    config.write()

    # Run the command
    if config['run_type'] == 'create':
        create_and_generate(
            bucket_name=config['bucket_name'],
            frequency_width=config['width'],
            ami_id=config['ami'],
            spot_price=config['spot_price_i3_4xlarge'],
            volume=config['volume'],
            frequencies_per_node=config['frequencies_per_node'],
            add_shutdown=config['shutdown'],
            iterations=config['iterations'],
            arcsec=config['arcsec'] + 'arcsec',
            w_projection_planes=config['w_projection_planes'],
            robust=config['robust'],
            only_image=config['only_image'],
            image_size=config['image_size'],
            clean_channel_average=config['clean_channel_average'],
            frequency_range=config['frequency_range'],
            clean_directory_name=config['clean_directory_name'],
            log_level=config['log_level'],
            produce_qa=config['produce_qa'],
            uvsub_directory_name=config['uvsub_directory_name'],
            fits_directory_name=config['fits_directory_name'],
            clean_tclean=config['clean_tclean'],
            run_note=config['run_note_clean'],
            use_bash=config['use_bash'],
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
            image_size=config['image_size'],
            clean_channel_average=config['clean_channel_average'],
            frequency_range=config['frequency_range'],
            clean_directory_name=config['clean_directory_name'],
            only_image=config['only_image'],
            produce_qa=config['produce_qa'],
            uvsub_directory_name=config['uvsub_directory_name'],
            fits_directory_name=config['fits_directory_name'],
            clean_tclean=config['clean_tclean'],
            run_note=config['run_note_clean'],
            use_bash=config['use_bash'],
        )
    else:
        generate_json(
            frequency_width=config['width'],
            bucket_name=config['bucket_name'],
            iterations=config['iterations'],
            arcsec=config['arcsec'] + 'arcsec',
            nodes=config['nodes'],
            volume=config['volume'],
            parallel_streams=PARALLEL_STREAMS,
            shutdown=config['shutdown'],
            w_projection_planes=config['w_projection_planes'],
            robust=config['robust'],
            image_size=config['image_size'],
            clean_channel_average=config['clean_channel_average'],
            frequency_range=config['frequency_range'],
            clean_directory_name=config['clean_directory_name'],
            only_image=config['only_image'],
            produce_qa=config['produce_qa'],
            uvsub_directory_name=config['uvsub_directory_name'],
            fits_directory_name=config['fits_directory_name'],
            clean_tclean=config['clean_tclean'],
            run_note=config['run_note_clean'],
            use_bash=config['use_bash'],
        )


def parser_arguments(command_line=sys.argv[1:]):
    # TODO: Add all the arguments
    parser = argparse.ArgumentParser('Build the CLEAN physical graph for a day')

    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('bucket', help='the bucket to access')
    common_parser.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    common_parser.add_argument('arcsec', help='the number of arcsec', default='2')
    common_parser.add_argument('--only_image', action='store_true', help='store only the image to S3', default=True)
    common_parser.add_argument('--width', type=int, help='the frequency width', default=4)
    common_parser.add_argument('--shutdown', action='store_true', help='add a shutdown drop', default=True)
    common_parser.add_argument('--iterations', type=int, help='the number of iterations', default=10)
    common_parser.add_argument('-v', '--verbosity', action='count', help='increase output verbosity', default=0)
    common_parser.add_argument('--w_projection_planes', type=int, help='the number of w projections planes', default=24)
    common_parser.add_argument('--robust', type=float, help='the robust value for clean', default=0.8)
    common_parser.add_argument('--image_size', type=int, help='the image size for clean', default=4096)

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
    parser_use.add_argument('host', help='the host the DALiuGE is running on')
    parser_use.add_argument('--port', type=int, help='the port to bind to', default=DIM_PORT)
    parser_use.set_defaults(func=command_use)

    parser_interactive = subparsers.add_parser('interactive', help='prompt the user for parameters and then run')
    parser_interactive.set_defaults(func=command_interactive)

    args = parser.parse_args(command_line)
    return args


if __name__ == '__main__':
    # json 13b-266 /mnt/daliuge/dlg_root 8 -w 8 -s
    logging.basicConfig(level=logging.INFO)
    arguments = parser_arguments()
    arguments.func(arguments)
