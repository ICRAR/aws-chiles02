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
import getpass
import json
import logging
import os
from time import sleep

from configobj import ConfigObj
from dlg.droputils import get_roots
from dlg.manager.client import DataIslandManagerClient

from aws_chiles02.common import get_aws_credentials, get_input_mode, get_session_id, get_uuid
from aws_chiles02.ec2_controller import EC2Controller
from aws_chiles02.generate_common import build_hosts, get_reported_running
from aws_chiles02.get_argument import GetArguments
from aws_chiles02.settings_file import AWS_AMI_ID, AWS_REGION, DIM_PORT, WAIT_TIMEOUT_ISLAND_MANAGER, \
    WAIT_TIMEOUT_NODE_MANAGER
from aws_chiles02.user_data import get_data_island_manager_user_data, get_node_manager_user_data
from build_graph_common import AbstractBuildGraph

LOG = logging.getLogger(__name__)
PARALLEL_STREAMS = 8


class BuildTestGraph(AbstractBuildGraph):
    def new_carry_over_data(self):
        pass

    def __init__(self, **keywords):
        super(BuildTestGraph, self).__init__(**keywords)
        self._parallel_streams = keywords['parallel_streams']
        self._use_bash = keywords['use_bash']
        self._casa_version = keywords['casa_version']
        self._test_directory_name=keywords['test_directory_name'],

    def build_graph(self):
        self.copy_parameter_data(self._test_directory_name)
        self.copy_logfiles_and_shutdown(self._test_directory_name)


def create_and_generate(**keywords):
    boto_data = get_aws_credentials('aws-chiles02')
    if boto_data is not None:
        bucket_name = keywords['bucket_name']
        nodes = keywords['nodes']
        spot_price = keywords['spot_price']
        ami_id = keywords['ami_id']

        uuid = get_uuid()
        ec2_data = EC2Controller(
            ami_id,
            [
                {
                    'number_instances': nodes,
                    'instance_type': 'm4.large',
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
                    'Value': 'DALiuGE NM - Test',
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
                        'Value': 'DALiuGE DIM - Test',
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

                graph = BuildTestGraph(
                    bucket_name=bucket_name,
                    volume=keywords['volume'],
                    parallel_streams=PARALLEL_STREAMS,
                    node_details=reported_running,
                    shutdown=keywords['add_shutdown'],
                    session_id=session_id,
                    dim_ip=host,
                    run_note=keywords['run_note'],
                    use_bash=keywords['use_bash'],
                    casa_version=keywords['casa_version'],
                    test_directory_name=keywords['test_directory_name'],
                )
                graph.build_graph()

                LOG.info('Connection to {0}:{1}'.format(host, DIM_PORT))
                client = DataIslandManagerClient(host, DIM_PORT)

                client.create_session(session_id)
                client.append_graph(session_id, graph.drop_list)
                client.deploy_session(session_id, get_roots(graph.drop_list))
    else:
        LOG.error('Unable to find the AWS credentials')


def generate_json(**keywords):
    bucket_name = keywords['bucket_name']

    node_details = {
        'i2.2xlarge': [{'ip_address': 'node_i2_{0}'.format(i)} for i in range(0, keywords['nodes'])]
    }

    graph = BuildTestGraph(
        bucket_name=bucket_name,
        volume=keywords['volume'],
        parallel_streams=PARALLEL_STREAMS,
        node_details=node_details,
        shutdown=keywords['add_shutdown'],
        session_id='session_id',
        dim_ip='1.2.3.4',
        run_note=keywords['run_note'],
        use_bash=keywords['use_bash'],
        casa_version=keywords['casa_version'],
        test_directory_name=keywords['test_directory_name'],
    )
    graph.build_graph()
    json_dumps = json.dumps(graph.drop_list, indent=2)
    LOG.info(json_dumps)
    with open(keywords.get('json_path', "/tmp/json_imageconcat.txt"), "w") as json_file:
        json_file.write(json_dumps)


def command_interactive():
    sleep(0.5)  # Allow the logging time to print
    path_dirname, filename = os.path.split(__file__)
    config_file_name = '{0}/aws-chiles02.settings'.format(path_dirname)
    if os.path.exists(config_file_name):
        config = ConfigObj(config_file_name)
    else:
        config = ConfigObj()
        config.filename = config_file_name

    mode = get_input_mode()
    args = GetArguments(config=config, mode=mode)
    args.get('run_type', 'Create or json', allowed=['create', 'json'], help_text='the use a network or create a network')
    args.get('bucket_name', 'Bucket name', help_text='the bucket to access', default='13b-266')
    args.get('volume', 'Volume', help_text='the directory on the host to bind to the Docker Apps')
    args.get('shutdown', 'Add the shutdown node', data_type=bool, help_text='add a shutdown drop', default=True)
    args.get('ami', 'AMI Id', help_text='the AMI to use', default=AWS_AMI_ID)
    args.get('spot_price_m4_large', 'Spot Price for m4.large', help_text='the spot price')
    args.get('nodes', 'Number of nodes', data_type=int, help_text='the number of nodes', default=1)
    args.get('run_note_test', 'A single line note about this run', help_text='A single line note about this run', default='No note')
    args.get('test_directory_name', 'The directory name for test', help_text='the directory name for test')
    args.get('use_bash', 'Run CASA in Bash rather than Docker', data_type=bool, help_text='run casa in bash', default=True)
    if config['use_bash']:
        args.get('casa_version', 'Which version of CASA', allowed=['4.7', '5.1'], help_text='the version of CASA', default='5.1')

    # Write the arguments
    config.write()

    # Run the command
    if config['run_type'] == 'create':
        create_and_generate(
            ami_id=config['ami'],
            spot_price=config['spot_price_m4_large'],
            nodes=config['nodes'],
            bucket_name=config['bucket_name'],
            volume=config['volume'],
            add_shutdown=config['shutdown'],
            run_note=config['run_note_test'],
            use_bash=config['use_bash'],
            casa_version=config['casa_version'],
            test_directory_name=config['test_directory_name'],
        )
    else:
        generate_json(
            ami_id=config['ami'],
            spot_price=config['spot_price_m4_large'],
            nodes=config['nodes'],
            bucket_name=config['bucket_name'],
            volume=config['volume'],
            add_shutdown=config['shutdown'],
            run_note=config['run_note_test'],
            use_bash=config['use_bash'],
            casa_version=config['casa_version'],
            test_directory_name=config['test_directory_name'],
        )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    command_interactive()