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
            if key.key.endswith('.tar'):
                cleaned_objects.append(key.key)
                LOG.info('{0} found'.format(key.key))

        uvsub_frequencies = []
        for key in bucket.objects.filter(Prefix='{0}'.format(self._s3_uvsub_name)):
            if key.key.endswith('.tar'):
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
        'instance_type': 'i3.8xlarge',
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
                get_node_manager_user_data(
                    boto_data,
                    uuid,
                    log_level=log_level,
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
                        wait=WAIT_TIMEOUT_ISLAND_MANAGER,
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
                        clean_weighting_uv=keywords['clean_weighting_uv'],
                        robust=keywords['robust'],
                        image_size=keywords['image_size'],
                        clean_channel_average=keywords['clean_channel_average'],
                        region_file=keywords['region_file'],
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
                        casa_version=keywords['casa_version'],
                        build_fits=keywords['build_fits'],
                    )
                    graph.build_graph()

                    LOG.info('Connection to {0}:{1}'.format(host, DIM_PORT))
                    client = DataIslandManagerClient(host, DIM_PORT, timeout=30)

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
            msg = 'Error while processing GET request for {0}:{1}/api (status {2}): {3}'.format(
                host,
                port,
                response.status,
                response.read())
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
                clean_weighting_uv=keywords['clean_weighting_uv'],
                robust=keywords['robust'],
                image_size=keywords['image_size'],
                clean_channel_average=keywords['clean_channel_average'],
                region_file=keywords['region_file'],
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
                casa_version=keywords['casa_version'],
                build_fits=keywords['build_fits'],
            )
            graph.build_graph()

            LOG.info('Connection to {0}:{1}'.format(host, port))
            client = DataIslandManagerClient(host, port, timeout=30)

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
        'i3.8xlarge': [{'ip_address': 'node_i2_{0}'.format(i)} for i in range(0, keywords['nodes'])]
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
        clean_weighting_uv=keywords['clean_weighting_uv'],
        robust=keywords['robust'],
        image_size=keywords['image_size'],
        clean_channel_average=keywords['clean_channel_average'],
        region_file=keywords['region_file'],
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
        build_fits=keywords['build_fits'],
        casa_version=keywords['casa_version'],
    )
    graph.build_graph()
    json_dumps = json.dumps(graph.drop_list, indent=2)
    LOG.info(json_dumps)
    with open(keywords.get('json_path', "/tmp/json_clean.txt"), "w") as json_file:
        json_file.write(json_dumps)


def command_interactive():
    path_dirname, filename = os.path.split(__file__)
    config_file_name = '{0}/aws-chiles02.settings'.format(path_dirname)
    if os.path.exists(config_file_name):
        config = ConfigObj(config_file_name)
    else:
        config = ConfigObj()
        config.filename = config_file_name

    # TODO: Pick the option you want

    args = GetArguments(config=config)
    args.get('run_type',
             'Create, use or json',
             allowed=['create', 'use', 'json'],
             help_text='the use a network or create a network')
    args.get('bucket_name', 'Bucket name', help_text='the bucket to access', default='13b-266')
    args.get('width', 'Frequency width', data_type=int, help_text='the frequency width', default=4)
    args.get('iterations', 'Clean iterations', data_type=int, help_text='the clean iterations', default=1)
    args.get('arcsec', 'How many arc seconds', help_text='the arc seconds', default='2')
    args.get('w_projection_planes',
             'W Projection planes',
             data_type=int,
             help_text='the number of w projections planes',
             default=24)
    args.get('clean_weighting_uv',
             'Clean weighting method',
             allowed=['briggs', 'uniform', 'natural'],
             help_text='which method for weighting the UV',
             default='briggs')
    if config['clean_weighting_uv'] == 'briggs':
        args.get('robust', 'Clean robust value', data_type=float, help_text='the robust value for clean', default=0.8)
    args.get('image_size', 'The image size', data_type=int, help_text='the image size for clean', default=4096)
    args.get('clean_channel_average',
             'The number of input channels to average',
             data_type=int,
             help_text='the number of input channels to average',
             default=1)
    args.get('region_file', 'Region File for cleaning', help_text='Region File for cleaning in crtf', default='')
    args.get('only_image', 'Only the image to S3', data_type=bool, help_text='only copy the image to S3', default=False)
    args.get('shutdown', 'Add the shutdown node', data_type=bool, help_text='add a shutdown drop', default=True)
    args.get('build_fits',
             'Build the fits files for JPEG2000 (yes or no)',
             allowed=['yes', 'no'],
             help_text='build the fits files for JPEG2000',
             default='no')
    args.get('uvsub_directory_name',
             'The directory name for the uvsub output',
             help_text='the directory name for the uvsub output')
    args.get('clean_directory_name', 'The directory name for clean', help_text='the directory name for clean')
    if config['build_fits'] == 'yes':
        args.get('fits_directory_name', 'The directory name for fits files', help_text='the directory name for fits')
    args.get('produce_qa',
             'Produce QA products (yes or no)',
             allowed=['yes', 'no'],
             help_text='should we produce the QA products')
    args.get('clean_tclean',
             'Clean or Tclean',
             allowed=['clean', 'tclean'],
             help_text='use clean or tclean',
             default='clean')
    args.get('use_bash',
             'Run CASA in Bash rather than Docker',
             data_type=bool,
             help_text='run casa in bash',
             default=True)
    if config['use_bash']:
        args.get('casa_version',
                 'Which version of CASA',
                 allowed=['4.7', '5.1'],
                 help_text='the version of CASA',
                 default='5.1')
    args.get('volume',
             'Volume',
             help_text='the directory on the host to bind to the Docker Apps and where file/container drops go',
             default='/mnt/daliuge/dlg_root')
    args.get('frequency_range',
             'Do you want to specify a range of frequencies',
             help_text='Do you want to specify a range of frequencies comma separated',
             default='')
    args.get('run_note_clean',
             'A single line note about this run',
             help_text='A single line note about this run',
             default='No note')

    if config['run_type'] == 'create':
        args.get('ami', 'AMI Id', help_text='the AMI to use', default=AWS_AMI_ID)
        args.get('spot_price_i3_8xlarge', 'Spot Price for i3.8xlarge', help_text='the spot price')
        args.get('frequencies_per_node',
                 'Number of frequencies per node',
                 data_type=int,
                 help_text='the number of frequencies per node',
                 default=1)
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
            spot_price=config['spot_price_i3_8xlarge'],
            volume=config['volume'],
            frequencies_per_node=config['frequencies_per_node'],
            add_shutdown=config['shutdown'],
            iterations=config['iterations'],
            arcsec=config['arcsec'] + 'arcsec',
            w_projection_planes=config['w_projection_planes'],
            clean_weighting_uv=config['clean_weighting_uv'],
            robust=config['robust'],
            only_image=config['only_image'],
            image_size=config['image_size'],
            clean_channel_average=config['clean_channel_average'],
            region_file=config['region_file'],
            frequency_range=config['frequency_range'],
            clean_directory_name=config['clean_directory_name'],
            log_level=config['log_level'],
            produce_qa=config['produce_qa'],
            uvsub_directory_name=config['uvsub_directory_name'],
            fits_directory_name=config['fits_directory_name'],
            clean_tclean=config['clean_tclean'],
            run_note=config['run_note'],
            use_bash=config['use_bash'],
            casa_version=config['casa_version'],
            build_fits=config['build_fits'],
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
            clean_weighting_uv=config['clean_weighting_uv'],
            robust=config['robust'],
            image_size=config['image_size'],
            clean_channel_average=config['clean_channel_average'],
            region_file=config['region_file'],
            frequency_range=config['frequency_range'],
            clean_directory_name=config['clean_directory_name'],
            only_image=config['only_image'],
            produce_qa=config['produce_qa'],
            uvsub_directory_name=config['uvsub_directory_name'],
            fits_directory_name=config['fits_directory_name'],
            clean_tclean=config['clean_tclean'],
            run_note=config['run_note'],
            use_bash=config['use_bash'],
            casa_version=config['casa_version'],
            build_fits=config['build_fits'],
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
            clean_weighting_uv=config['clean_weighting_uv'],
            robust=config['robust'],
            image_size=config['image_size'],
            clean_channel_average=config['clean_channel_average'],
            region_file=config['region_file'],
            frequency_range=config['frequency_range'],
            clean_directory_name=config['clean_directory_name'],
            only_image=config['only_image'],
            produce_qa=config['produce_qa'],
            uvsub_directory_name=config['uvsub_directory_name'],
            fits_directory_name=config['fits_directory_name'],
            clean_tclean=config['clean_tclean'],
            run_note=config['run_note'],
            use_bash=config['use_bash'],
            casa_version=config['casa_version'],
            build_fits=config['build_fits'],
        )


if __name__ == '__main__':
    # json 13b-266 /mnt/daliuge/dlg_root 8 -w 8 -s
    logging.basicConfig(level=logging.INFO)
    command_interactive()
