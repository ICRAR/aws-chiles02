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
Perform the image concatentation
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

from aws_chiles02.build_graph_imageconcat import BuildGraphImageconcat
from aws_chiles02.common import ChunkedFrequencyPair, TKINTER, get_aws_credentials, get_input_mode, get_list_frequency_groups, get_required_frequencies, get_session_id, get_uuid
from aws_chiles02.ec2_controller import EC2Controller
from aws_chiles02.generate_common import build_hosts, get_nodes_running, get_reported_running
from aws_chiles02.get_argument import GetArguments
from aws_chiles02.settings_file import AWS_AMI_ID, AWS_REGION, DIM_PORT, WAIT_TIMEOUT_NODE_MANAGER, WAIT_TIMEOUT_ISLAND_MANAGER
from aws_chiles02.user_data import get_data_island_manager_user_data, get_node_manager_user_data
from dlg.droputils import get_roots
from dlg.manager.client import DataIslandManagerClient

LOG = logging.getLogger(__name__)
PARALLEL_STREAMS = 8


class WorkToDo(object):
    def __init__(self, **keywords):
        self._bucket_name = keywords['bucket_name']
        self._frequency_width = keywords['frequency_width']
        self._imageconcat_width = keywords['imageconcat_width']
        self._s3_clean_name = keywords['clean_directory_name']
        self._s3_imageconcat_directory_name = keywords['imageconcat_directory_name']
        self._frequency_range = get_required_frequencies(keywords['frequency_range'], self._frequency_width)
        self._work_to_do = []
        self._cleaned_objects = []

    def calculate_work_to_do(self):
        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)

        imageconcat_objects = []
        bucket = s3.Bucket(self._bucket_name)
        for key in bucket.objects.filter(Prefix='{0}'.format(self._s3_imageconcat_directory_name)):
            imageconcat_objects.append(key.key)
            LOG.info('imageconcat {0} found'.format(key.key))

        for key in bucket.objects.filter(Prefix='{0}'.format(self._s3_clean_name)):
            if key.key.endswith('.tar.centre'):
                self._cleaned_objects.append(key.key)
                LOG.info('{0} found'.format(key.key))

        # Get work we've already done
        list_frequencies = get_list_frequency_groups(self._frequency_width)
        list_chunked_frequencies = self._chunk_up(list_frequencies)
        for frequency_pair in list_chunked_frequencies:
            # Use the frequency
            if self._frequency_range is not None and not self._pair_in_range(frequency_pair):
                continue

            expected_imageconcat_file = '{0}/image_{1}_{2}.tar'.format(
                self._s3_imageconcat_directory_name,
                frequency_pair.bottom_frequency,
                frequency_pair.top_frequency,
            )
            if expected_imageconcat_file not in imageconcat_objects:
                if self._some_clean_elements_present(frequency_pair, self._cleaned_objects):
                    self._work_to_do.append(frequency_pair)

    @property
    def work_to_do(self):
        return self._work_to_do

    @property
    def cleaned_objects(self):
        return self._cleaned_objects

    def _some_clean_elements_present(self, frequency_pair, cleaned_objects):
        for pair in frequency_pair.pairs:
            expected_tar_file = '{0}/cleaned_{1}_{2}.tar.centre'.format(
                self._s3_clean_name,
                pair.bottom_frequency,
                pair.top_frequency,
            )
            if expected_tar_file in cleaned_objects:
                return True

        return False

    def _chunk_up(self, list_frequencies):
        chunked_up = []
        elements = []
        counter = 1
        bottom_frequency = None
        for frequency_pair in list_frequencies:
            elements.append(frequency_pair)
            if bottom_frequency is None:
                bottom_frequency = frequency_pair.bottom_frequency

            if counter == self._imageconcat_width:
                chunk = ChunkedFrequencyPair(
                    bottom_frequency,
                    frequency_pair.top_frequency,
                    elements,
                )
                chunked_up.append(chunk)

                elements = [frequency_pair]
                bottom_frequency = frequency_pair.bottom_frequency
                counter = 1

            counter += 1

        if len(elements) >= 2:
            frequency_pair = list_frequencies[-1]
            chunk = ChunkedFrequencyPair(
                bottom_frequency,
                frequency_pair.top_frequency,
                elements,
            )
            chunked_up.append(chunk)

        return chunked_up

    def _pair_in_range(self, frequency_pair):
        for pair in frequency_pair.pairs:
            if pair in self._frequency_range:
                return True

        return False


def generate_json(**keywords):
    frequency_width = keywords['frequency_width']
    bucket_name = keywords['bucket_name']
    clean_directory_name = keywords['clean_directory_name']
    fits_directory_name = keywords['fits_directory_name']
    imageconcat_directory_name = keywords['imageconcat_directory_name']
    work_to_do = WorkToDo(
        bucket_name=bucket_name,
        frequency_width=frequency_width,
        imageconcat_width=keywords['imageconcat_width'],
        frequency_range=keywords['frequency_range'],
        clean_directory_name=clean_directory_name,
        imageconcat_directory_name=imageconcat_directory_name,
    )
    work_to_do.calculate_work_to_do()

    node_details = {
        'i2.2xlarge': [{'ip_address': 'node_i2_{0}'.format(i)} for i in range(0, keywords['nodes'])]
    }

    graph = BuildGraphImageconcat(
        work_to_do=work_to_do.work_to_do,
        cleaned_objects=work_to_do.cleaned_objects,
        bucket_name=bucket_name,
        volume=keywords['volume'],
        parallel_streams=PARALLEL_STREAMS,
        node_details=node_details,
        shutdown=keywords['add_shutdown'],
        width=frequency_width,
        clean_directory_name=clean_directory_name,
        fits_directory_name=fits_directory_name,
        imageconcat_directory_name=imageconcat_directory_name,
        session_id='session_id',
        dim_ip='1.2.3.4',
        run_note=keywords['run_note'],
        use_bash=keywords['use_bash'],
        casa_version=keywords['casa_version'],
        build_fits=keywords['build_fits'],
    )
    graph.build_graph()
    json_dumps = json.dumps(graph.drop_list, indent=2)
    LOG.info(json_dumps)
    with open(keywords['json_path'] if 'json_path' in keywords else "/tmp/json_imageconcat.txt", "w") as json_file:
        json_file.write(json_dumps)


def create_and_generate(**keywords):
    boto_data = get_aws_credentials('aws-chiles02')
    if boto_data is not None:
        frequency_width = keywords['frequency_width']
        bucket_name = keywords['bucket_name']
        clean_directory_name = keywords['clean_directory_name']
        fits_directory_name = keywords['fits_directory_name']
        imageconcat_directory_name = keywords['imageconcat_directory_name']
        work_to_do = WorkToDo(
            bucket_name=bucket_name,
            frequency_width=frequency_width,
            imageconcat_width=keywords['imageconcat_width'],
            frequency_range=keywords['frequency_range'],
            clean_directory_name=clean_directory_name,
            imageconcat_directory_name=imageconcat_directory_name,
        )
        work_to_do.calculate_work_to_do()

        nodes = keywords['nodes']
        spot_price = keywords['spot_price']
        ami_id = keywords['ami_id']

        uuid = get_uuid()
        ec2_data = EC2Controller(
            ami_id,
            [
                {
                    'number_instances': nodes,
                    'instance_type': 'i3.xlarge',
                    'spot_price': spot_price
                }
            ],
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
                    'Value': 'DALiuGE NM - Imageconcat',
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
            nodes,
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
                        'Value': 'DALiuGE DIM - Imageconcat',
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

                graph = BuildGraphImageconcat(
                    work_to_do=work_to_do.work_to_do,
                    cleaned_objects=work_to_do.cleaned_objects,
                    bucket_name=bucket_name,
                    volume=keywords['volume'],
                    parallel_streams=PARALLEL_STREAMS,
                    node_details=reported_running,
                    shutdown=keywords['add_shutdown'],
                    width=frequency_width,
                    clean_directory_name=clean_directory_name,
                    fits_directory_name=fits_directory_name,
                    imageconcat_directory_name=imageconcat_directory_name,
                    session_id=session_id,
                    dim_ip=host,
                    run_note=keywords['run_note'],
                    use_bash=keywords['use_bash'],
                    casa_version=keywords['casa_version'],
                    build_fits=keywords['build_fits'],
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
            fits_directory_name = keywords['fits_directory_name']
            imageconcat_directory_name = keywords['imageconcat_directory_name']
            work_to_do = WorkToDo(
                bucket_name=bucket_name,
                frequency_width=frequency_width,
                imageconcat_width=keywords['imageconcat_width'],
                frequency_range=keywords['frequency_range'],
                clean_directory_name=clean_directory_name,
                imageconcat_directory_name=imageconcat_directory_name,
            )
            work_to_do.calculate_work_to_do()

            # Now build the graph
            session_id = get_session_id()
            graph = BuildGraphImageconcat(
                work_to_do=work_to_do.work_to_do,
                cleaned_objects=work_to_do.cleaned_objects,
                bucket_name=bucket_name,
                volume=keywords['volume'],
                parallel_streams=PARALLEL_STREAMS,
                node_details=nodes_running,
                shutdown=keywords['add_shutdown'],
                width=frequency_width,
                clean_directory_name=clean_directory_name,
                fits_directory_name=fits_directory_name,
                imageconcat_directory_name=imageconcat_directory_name,
                session_id=session_id,
                dim_ip=host,
                run_note=keywords['run_note'],
                use_bash=keywords['use_bash'],
                casa_version=keywords['casa_version'],
                build_fits=keywords['build_fits'],
            )
            graph.build_graph()
            LOG.info('Connection to {0}:{1}'.format(host, port))
            client = DataIslandManagerClient(host, port)

            client.create_session(session_id)
            client.append_graph(session_id, graph.drop_list)
            client.deploy_session(session_id, get_roots(graph.drop_list))

        else:
            LOG.warning('No nodes are running')


def command_create(args):
    create_and_generate(
        ami_id=args.ami_id,
        spot_price=args.spot_price,
        nodes=args.nodes,
        bucket_name=args.bucket_name,
        frequency_width=args.frequency_width,
        imageconcat_width=args.imageconcat_width,
        volume=args.volume,
        add_shutdown=args.add_shutdown,
        frequency_range=args.frequency_range,
        clean_directory_name=args.clean_directory_name,
        fits_directory_name=args.fits_directory_name,
        imageconcat_directory_name=args.imageconcat_directory_name,
        run_note=args.run_note_imageconcat,
    )


def command_use(args):
    use_and_generate(
        host=args.host,
        port=args.port,
        bucket_name=args.bucket_name,
        frequency_width=args.frequency_width,
        imageconcat_width=args.imageconcat_width,
        volume=args.volume,
        add_shutdown=args.add_shutdown,
        frequency_range=args.frequency_range,
        clean_directory_name=args.clean_directory_name,
        fits_directory_name=args.fits_directory_name,
        imageconcat_directory_name=args.imageconcat_directory_name,
        run_note=args.run_note_imageconcat,
    )


def command_json(args):
    generate_json(
        nodes=args.nodes,
        bucket_name=args.bucket_name,
        frequency_width=args.frequency_width,
        imageconcat_width=args.imageconcat_width,
        volume=args.volume,
        add_shutdown=args.add_shutdown,
        frequency_range=args.frequency_range,
        clean_directory_name=args.clean_directory_name,
        fits_directory_name=args.fits_directory_name,
        imageconcat_directory_name=args.imageconcat_directory_name,
        run_note=args.run_note_imageconcat,
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
        args.get('imageconcat_width', 'Image Concat Frequency width', data_type=int, help_text='the imageconcat frequency width', default=6)
        args.get('shutdown', 'Add the shutdown node', data_type=bool, help_text='add a shutdown drop', default=True)
        args.get('clean_directory_name', 'The directory name for clean', help_text='the directory name for clean')
        args.get('imageconcat_directory_name', 'The directory name for imageconcat', help_text='the directory name for imageconcat')
        args.get('build_fits', 'Build the fits files for JPEG2000 (yes or no)', allowed=['yes', 'no'], help_text='build the fits files for JPEG2000', default='no')
        if config['build_fits'] == 'yes':
            args.get('fits_directory_name', 'The directory name for fits files', help_text='the directory name for fits')
        args.get('use_bash', 'Run CASA in Bash rather than Docker', data_type=bool, help_text='run casa in bash', default=True)
        if config['use_bash']:
            args.get('casa_version', 'Which version of CASA', allowed=['4.7', '5.1'], help_text='the version of CASA', default='5.1')
        args.get('frequency_range', 'Do you want to specify a range of frequencies', help_text='Do you want to specify a range of frequencies comma separated', default='')
        args.get('run_note_imageconcat', 'A single line note about this run', help_text='A single line note about this run', default='No note')

        if config['run_type'] == 'create':
            args.get('ami', 'AMI Id', help_text='the AMI to use', default=AWS_AMI_ID)
            args.get('spot_price_i3_xlarge', 'Spot Price for i3.xlarge', help_text='the spot price')
            args.get('nodes', 'Number of nodes', data_type=int, help_text='the number of nodes', default=1)
        elif config['run_type'] == 'use':
            args.get('dim', 'Data Island Manager', help_text='the IP to the DataIsland Manager')
        else:
            args.get('nodes', 'Number of nodes', data_type=int, help_text='the number of nodes', default=1)

    # Write the arguments
    config.write()

    # Run the command
    if config['run_type'] == 'create':
        create_and_generate(
            ami_id=config['ami'],
            spot_price=config['spot_price_i3_xlarge'],
            nodes=config['nodes'],
            bucket_name=config['bucket_name'],
            frequency_width=config['width'],
            imageconcat_width=config['imageconcat_width'],
            volume=config['volume'],
            add_shutdown=config['shutdown'],
            frequency_range=config['frequency_range'],
            clean_directory_name=config['clean_directory_name'],
            fits_directory_name=config['fits_directory_name'],
            imageconcat_directory_name=config['imageconcat_directory_name'],
            run_note=config['run_note_imageconcat'],
            use_bash=config['use_bash'],
            casa_version=config['casa_version'],
        )
    elif config['run_type'] == 'use':
        use_and_generate(
            host=config['dim'],
            port=DIM_PORT,
            bucket_name=config['bucket_name'],
            frequency_width=config['width'],
            imageconcat_width=config['imageconcat_width'],
            volume=config['volume'],
            add_shutdown=config['shutdown'],
            frequency_range=config['frequency_range'],
            clean_directory_name=config['clean_directory_name'],
            fits_directory_name=config['fits_directory_name'],
            imageconcat_directory_name=config['imageconcat_directory_name'],
            run_note=config['run_note_imageconcat'],
            use_bash=config['use_bash'],
            casa_version=config['casa_version'],
        )
    else:
        generate_json(
            nodes=config['nodes'],
            bucket_name=config['bucket_name'],
            frequency_width=config['width'],
            imageconcat_width=config['imageconcat_width'],
            volume=config['volume'],
            add_shutdown=config['shutdown'],
            frequency_range=config['frequency_range'],
            clean_directory_name=config['clean_directory_name'],
            fits_directory_name=config['fits_directory_name'],
            imageconcat_directory_name=config['imageconcat_directory_name'],
            run_note=config['run_note_imageconcat'],
            use_bash=config['use_bash'],
            casa_version=config['casa_version'],
        )


def parser_arguments(command_line=sys.argv[1:]):
    # TODO: Add all the arguments
    parser = argparse.ArgumentParser('Build the concatenated image ')

    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('bucket', help='the bucket to access')
    common_parser.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    common_parser.add_argument('arcsec', help='the number of arcsec', default='2')
    common_parser.add_argument('--width', type=int, help='the frequency width', default=4)
    common_parser.add_argument('--shutdown', action='store_true', help='add a shutdown drop', default=True)
    common_parser.add_argument('-v', '--verbosity', action='count', help='increase output verbosity', default=0)

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
    logging.basicConfig(level=logging.INFO)
    arguments = parser_arguments()
    arguments.func(arguments)
