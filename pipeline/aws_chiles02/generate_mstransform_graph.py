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
import json
import logging
import os

import boto3
import time
from configobj import ConfigObj

from aws_chiles02.build_graph import BuildGraph
from aws_chiles02.common import get_session_id, get_list_frequency_groups, FrequencyPair, get_argument, get_user_data, get_aws_credentials, get_file_contents
from aws_chiles02.ec2_controller import EC2Controller
from aws_chiles02.settings_file import INPUT_MS_SUFFIX_TAR, AWS_REGION, AWS_AMI_ID
from dfms.manager.client import NodeManagerClient, SetEncoder

LOG = logging.getLogger(__name__)
_1GB = 1073741824
QUEUE = 'startup_complete'


class MeasurementSetData:
    def __init__(self, full_tar_name, size):
        self.full_tar_name = full_tar_name
        self.size = size
        # Get rid of the '_calibrated_deepfield.ms.tar'
        self.short_name = full_tar_name[:-len(INPUT_MS_SUFFIX_TAR)]

    def __str__(self):
        return '{0}: {1}'.format(self.short_name, self.size)

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash((self.full_tar_name, self.size))

    def __eq__(self, other):
        return (self.full_tar_name, self.size) == (other.full_tar_name, other.size)


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
        for key in self._bucket.objects.all():
            if key.key.endswith('_calibrated_deepfield.ms.tar'):
                obj = s3.Object(key.bucket_name, key.key)
                storage_class = obj.storage_class
                restore = obj.restore
                LOG.info('{0}, {1}, {2}'.format(key.key, storage_class, restore))

                ok_to_queue = True
                if 'GLACIER' == storage_class:
                    if restore is None or restore.startswith('ongoing-request="true"'):
                        ok_to_queue = False

                if ok_to_queue:
                    list_measurement_sets.append(MeasurementSetData(key.key, key.size))

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
                if frequency_group.name not in splits_done:
                    frequency_groups.append(frequency_group)

        return frequency_groups

    def _ignore_day(self, list_frequency_groups):
        if len(list_frequency_groups) >= 4:
            return False

        # Check if we have the first groups
        count_bottom = 0
        for bottom_frequency in range(940, 952, self._width):
            frequency_group = FrequencyPair(bottom_frequency, bottom_frequency + self._width)
            if frequency_group in list_frequency_groups:
                count_bottom += 1

        return len(list_frequency_groups) - count_bottom <= 0

    @property
    def work_to_do(self):
        return self._work_to_do


def get_s3_split_name(width):
    return 'split_{}'.format(width)


def get_all_user_data(boto_data, session_id):
    cloud_init = get_file_contents('dfms_cloud_init.yaml')
    cloud_init_dynamic = '''#cloud-config
# Write the boto file
write_files:
  - path: "/root/.aws/credentials"
    permissions: "0544"
    owner: "root"
    content: |
      [{0}]
      aws_access_key_id = {1}
      aws_secret_access_key = {2}
  - path: "/home/ec2-user/.aws/credentials"
    permissions: "0544"
    owner: "ec2-user:ec2-user"
    content: |
      [{0}]
      aws_access_key_id = {1}
      aws_secret_access_key = {2}
'''.format(
        'aws-chiles02',
        boto_data[0],
        boto_data[1],
    )
    user_script = get_file_contents('node_manager_start_up.bash')
    dynamic_script = '''#!/bin/bash -vx
runuser -l ec2-user -c 'cd /home/ec2-user/aws-chiles02/pipeline/aws_chiles02 && source /home/ec2-user/virtualenv/aws-chiles02/bin/activate && python startup_complete.py {1} us-west-2 "{0}"'''\
        .format(session_id, QUEUE)
    user_data = get_user_data([cloud_init, cloud_init_dynamic, user_script, dynamic_script])
    return user_data


def get_nodes_required(days, days_per_node, spot_price1, spot_price2):
    nodes = []
    counts = [0, 0]
    for day in days:
        if day.size <= 500 * _1GB:
            counts[0] += 1
        else:
            counts[1] += 1

    if counts[0] > 0:
        nodes.append({
            'number_instances': max(counts[0] / days_per_node, 1),
            'instance_type': 'i2.2xlarge',
            'spot_price': spot_price1
        })
    if counts[1] > 0:
        nodes.append({
            'number_instances': max(counts[1] / days_per_node, 1),
            'instance_type': 'i2.4xlarge',
            'spot_price': spot_price2
        })

    return nodes


def get_reported_running(session_id):
    session = boto3.Session(profile_name='aws-chiles02')
    sqs = session.resource('sqs', region_name=AWS_REGION)
    queue = sqs.get_queue_by_name(QueueName=QUEUE)
    nodes_running ={}

    for message in queue.receive_messages(MessageAttributeNames=['session_id']):
        if message.message_attributes is not None:
            message_session_id = message.message_attributes.get('session_id').get('StringValue')
            if message_session_id == session_id:
                json_message = message.body
                message_details = json.loads(json_message)

                ip_addresses = nodes_running.get(message_details['instance_type'])
                if ip_addresses is None:
                    ip_addresses = []
                    nodes_running[message_details['instance_type']] = ip_addresses
                ip_addresses.append(message_details['ip_address'])

                message.delete()

    return nodes_running


def run_the_generate(bucket_name, frequency_width, ami_id, spot_price1, spot_price2, volume, days_per_node, add_shutdown):
    boto_data = get_aws_credentials('aws-chiles02')
    if boto_data is not None:
        session_id = get_session_id()

        work_to_do = WorkToDo(frequency_width, bucket_name, get_s3_split_name(frequency_width))
        work_to_do.calculate_work_to_do()

        days = work_to_do.work_to_do.keys()
        nodes_required = get_nodes_required(days, days_per_node, spot_price1, spot_price2)

        if len(nodes_required) > 0:
            ec2_data = EC2Controller(
                ami_id,
                nodes_required,
                get_all_user_data(boto_data, session_id),
                AWS_REGION,
                tags=[
                    {
                        'Key': 'Owner',
                        'Value': 'Kevin',
                    },
                    {
                        'Key': 'Name',
                        'Value': 'DFMS Node',
                    },
                    {
                        'Key': 'SessionId',
                        'Value': session_id,
                    }
                ]

            )
            ec2_data.start_instances()

            time.sleep(100)
            reported_running = get_reported_running(session_id)

            graph = BuildGraph(work_to_do.work_to_do, bucket_name, volume, ec2_data.running_nodes, add_shutdown, frequency_width)
            graph.build_graph()

            client = NodeManagerClient(args.host, args.port)

            client.create_session(session_id)
            client.append_graph(session_id, graph.drop_list)
            client.deploy_session(session_id, graph.start_oids)
    else:
        LOG.error('Unable to find the AWS credentials')


def command_json(args):
    work_to_do = WorkToDo(args.width, args.bucket, get_s3_split_name(args.width))
    work_to_do.calculate_work_to_do()

    graph = BuildGraph(work_to_do.work_to_do, args.bucket, args.volume, args.parallel_streams, args.nodes, args.shutdown, args.width)
    graph.build_graph()
    json_dumps = json.dumps(graph.drop_list, indent=2, cls=SetEncoder)
    LOG.info(json_dumps)
    with open("/tmp/json_split.txt", "w") as json_file:
        json_file.write(json_dumps)


def command_run(args):
    run_the_generate(
        args.bucket,
        args.width,
        args.ami,
        args.spot_price1,
        args.spot_price2,
        args.volume,
        args.days_per_node,
        args.shutdown,
    )


def command_interactive(args):
    LOG.info(args)
    path_dirname, filename = os.path.split(__file__)
    root, ext = os.path.splitext(filename)
    config_file_name = '{0}/{1}.settings'.format(path_dirname, root)
    if os.path.exists(config_file_name):
        config = ConfigObj(config_file_name)
    else:
        config = ConfigObj()
        config.filename = config_file_name

    get_argument(config, 'ami', 'AMI Id', help_text='the AMI to use', default=AWS_AMI_ID)
    get_argument(config, 'spot_price1', 'Spot Price for i2.2xlarge', help_text='the spot price')
    get_argument(config, 'spot_price2', 'Spot Price for i2.4xlarge', help_text='the spot price')
    get_argument(config, 'bucket_name', 'Bucket name', help_text='the bucket to access', default='13b-266')
    get_argument(config, 'volume', 'Volume', help_text='the directory on the host to bind to the Docker Apps')
    get_argument(config, 'width', 'Frequency width', data_type=int, help_text='the frequency width', default=4)
    get_argument(config, 'days_per_node', 'Number of days per node', data_type=int, help_text='the number of days per node', default=1)
    get_argument(config, 'shutdown', 'Add the shutdown node', data_type=bool, help_text='add a shutdown drop', default=True)

    # Write the arguments
    config.write()

    # Run the command
    run_the_generate(
        config['bucket_name'],
        config['width'],
        config['ami'],
        config['spot_price1'],
        config['spot_price2'],
        config['volume'],
        config['days_per_node'],
        config['shutdown'],
    )


def parser_arguments():
    parser = argparse.ArgumentParser('Build the MSTRANSFORM physical graph for a day')

    subparsers = parser.add_subparsers()

    parser_json = subparsers.add_parser('json', help='display the json')
    parser_json.add_argument('bucket', help='the bucket to access')
    parser_json.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    parser_json.add_argument('parallel_streams', type=int, help='the of parallel streams')
    parser_json.add_argument('-w', '--width', type=int, help='the frequency width', default=4)
    parser_json.add_argument('-n', '--nodes', type=int, help='the number of nodes', default=1)
    parser_json.add_argument('-s', '--shutdown', action="store_true", help='add a shutdown drop')
    parser_json.set_defaults(func=command_json)

    parser_run = subparsers.add_parser('run', help='run and deploy')
    parser_run.add_argument('ami', help='the ami to use')
    parser_run.add_argument('spot_price1', type=float, help='the spot price')
    parser_run.add_argument('spot_price2', type=float, help='the spot price')
    parser_run.add_argument('bucket', help='the bucket to access')
    parser_run.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    parser_run.add_argument('-w', '--width', type=int, help='the frequency width', default=4)
    parser_run.add_argument('-d', '--days_per_node', type=int, help='the number of days per node', default=1)
    parser_run.add_argument('-s', '--shutdown', action="store_true", help='add a shutdown drop')
    parser_run.set_defaults(func=command_run)

    parser_interactive = subparsers.add_parser('interactive', help='prompt the user for parameters and then run')
    parser_interactive.set_defaults(func=command_interactive)

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    # json 13b-266 /mnt/dfms/dfms_root 8 -w 8 -s
    # interactive
    logging.basicConfig(level=logging.INFO)
    arguments = parser_arguments()
    arguments.func(arguments)
