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
import operator

from aws_chiles02.common import get_oid, get_module_name, get_uid, get_session_id, get_observation, CONTAINER_JAVA_S3_COPY, CONTAINER_CHILES02, make_groups_of_frequencies, INPUT_MS_SUFFIX_TAR, \
    get_list_frequency_groups, FrequencyPair
from aws_chiles02.apps import DockerCopyFromS3, DockerCopyToS3, DockerMsTransform, DockerListobs, InitializeSqliteApp
from dfms.drop import DirectoryContainer, dropdict, BarrierAppDROP
from dfms.manager.client import NodeManagerClient, SetEncoder

LOG = logging.getLogger(__name__)
_1GB = 1073741824


class MeasurementSetData:
    def __init__(self, full_tar_name, size):
        self.full_tar_name = full_tar_name
        self.size = size
        # Get rid of the '_calibrated_deepfield.ms.tar'
        self.short_name = full_tar_name[:-len(INPUT_MS_SUFFIX_TAR)]


def get_details_for_measurement_set(splits_done, list_frequencies):
    frequency_groups = []
    if splits_done is None:
        frequency_groups.extend(list_frequencies)
    else:
        # Remove the groups we've processed
        for frequency_group in list_frequencies:
            if frequency_group.name not in splits_done:
                frequency_groups.append(frequency_group)

    return frequency_groups


def get_work_already_done(bucket):
    frequencies_per_day = {}
    for key in bucket.objects.filter(Prefix='split'):
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


def ignore_day(list_frequency_groups, frequency_width):
    if len(list_frequency_groups) >= 4:
        return False

    # Check if we have the first groups
    count_bottom = 0
    for bottom_frequency in range(940, 952, frequency_width):
        frequency_group = FrequencyPair(bottom_frequency, bottom_frequency + frequency_width)
        if frequency_group in list_frequency_groups:
            count_bottom += 1

    return len(list_frequency_groups) - count_bottom <= 0


def build_graph(args):
    session = boto3.Session(profile_name='aws-chiles02')
    s3 = session.resource('s3', use_ssl=False)

    list_measurement_sets = []
    bucket = s3.Bucket(args.bucket)
    for key in bucket.objects.all():
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
                # For testing
                if key.size <= 200 * _1GB:
                    queue = 'xlarge'
                elif key.size <= 400 * _1GB:
                    queue = '2xlarge'
                else:
                    queue = '4xlarge'

                LOG.info('{0}, {1}, {2}'.format(key.key, key.size, queue))

                if queue == args.ami_size:
                    list_measurement_sets.append(MeasurementSetData(key.key, key.size))

    # Make sure the smallest is first
    sorted_list_measurement_sets = sorted(list_measurement_sets, key=operator.attrgetter('size'))

    # Get work we've already done
    list_frequencies = get_list_frequency_groups(args.width)
    work_already_done = get_work_already_done(bucket)

    full_drop_list = []
    barrier_drop = None
    properties = None

    sqlite01 = get_oid('sqlite')
    sqlite_drop = dropdict({
        "type": 'plain',
        "storage": 'file',
        "oid": sqlite01,
        "uid": get_uid(),
        "precious": False,
        "dirname": os.path.join(args.volume, sqlite01),
        "check_exists": False,
    })
    sqlite_s3_drop = dropdict({
        "type": 'plain',
        "storage": 's3',
        "oid": get_oid('s3'),
        "uid": get_uid(),
        "precious": False,
        "bucket": args.bucket,
        "key": 'database.sqlite',
        "profile_name": 'aws-chiles02',
    })
    initialize_sqlite = dropdict({
        "type": 'app',
        "app": get_module_name(InitializeSqliteApp),
        "oid": get_oid('app'),
        "uid": get_uid(),
        "user": 'root',
    })
    sqlite_in_memory = dropdict({
        "type": 'plain',
        "storage": 'memory',
        "oid": get_oid('memory'),
        "uid": get_uid(),
        "precious": False,
    })
    initialize_sqlite.addInput(sqlite_drop)
    initialize_sqlite.addOutput(sqlite_in_memory)

    full_drop_list.append(sqlite_drop)
    full_drop_list.append(sqlite_s3_drop)
    full_drop_list.append(initialize_sqlite)

    for day_to_process in sorted_list_measurement_sets:
        day_work_already_done = work_already_done.get(day_to_process.short_name)
        list_frequency_groups = get_details_for_measurement_set(day_work_already_done, list_frequencies)

        if ignore_day(list_frequency_groups, args.width):
            LOG.info('{0} has already been process.'.format(day_to_process.full_tar_name))
        else:
            frequency_groups = make_groups_of_frequencies(list_frequency_groups, args.cores)

            drop_list = []
            s3_drop = dropdict({
                "type": 'plain',
                "storage": 's3',
                "oid": get_oid('s3'),
                "uid": get_uid(),
                "precious": False,
                "bucket": args.bucket,
                "key": day_to_process.full_tar_name,
                "profile_name": 'aws-chiles02',
            })
            if properties is not None:
                s3_drop.addInput(properties)
            else:
                s3_drop.addInput(sqlite_in_memory)

            copy_from_s3 = dropdict({
                "type": 'app',
                "app": get_module_name(DockerCopyFromS3),
                "oid": get_oid('app'),
                "uid": get_uid(),
                "image": CONTAINER_JAVA_S3_COPY,
                "command": 'copy_from_s3',
                "additionalBindings": ['/home/ec2-user/.aws/credentials:/root/.aws/credentials'],
                "user": 'root',
            })
            oid01 = get_oid('dir')
            measurement_set = dropdict({
                "type": 'container',
                "container": get_module_name(DirectoryContainer),
                "oid": oid01,
                "uid": get_uid(),
                "precious": False,
                "dirname": os.path.join(args.volume, oid01),
                "check_exists": False,
            })

            copy_from_s3.addInput(s3_drop)
            copy_from_s3.addOutput(measurement_set)

            drop_list.append(s3_drop)
            drop_list.append(copy_from_s3)
            drop_list.append(measurement_set)

            drop_listobs = dropdict({
                "type": 'app',
                "app": get_module_name(DockerListobs),
                "oid": get_oid('app'),
                "uid": get_uid(),
                "image": CONTAINER_CHILES02,
                "command": 'listobs',
                "user": 'root'
            })

            oid02 = get_oid('json')
            properties = dropdict({
                "type": 'plain',
                "storage": 'json',
                "oid": oid02,
                "uid": get_uid(),
                "precious": False,
                "dirname": os.path.join(args.volume, oid02),
                "check_exists": False,
            })

            drop_listobs.addInput(measurement_set)
            drop_listobs.addOutput(properties)
            if barrier_drop is not None:
                barrier_drop.addOutput(properties)

            drop_list.append(drop_listobs)
            drop_list.append(properties)

            outputs = []
            for group in frequency_groups:
                first = True
                end_of_last_element = None
                for frequency_pairs in group:
                    casa_py_drop = dropdict({
                        "type": 'app',
                        "app": get_module_name(DockerMsTransform),
                        "oid": get_oid('app'),
                        "uid": get_uid(),
                        "image": CONTAINER_CHILES02,
                        "command": 'mstransform',
                        "min_frequency": frequency_pairs.bottom_frequency,
                        "max_frequency": frequency_pairs.top_frequency,
                        "user": 'root',
                    })
                    oid03 = get_oid('dir')
                    result = dropdict({
                        "type": 'container',
                        "container": get_module_name(DirectoryContainer),
                        "oid": oid03,
                        "uid": get_uid(),
                        "precious": False,
                        "dirname": os.path.join(args.volume, oid03),
                        "check_exists": False,
                        "expireAfterUse": True,
                    })

                    casa_py_drop.addInput(measurement_set)
                    casa_py_drop.addInput(properties)
                    if first:
                        first = False
                    else:
                        casa_py_drop.addInput(end_of_last_element)
                    casa_py_drop.addOutput(result)

                    drop_list.append(casa_py_drop)
                    drop_list.append(result)

                    copy_to_s3 = dropdict({
                        "type": 'app',
                        "app": get_module_name(DockerCopyToS3),
                        "oid": get_oid('app'),
                        "uid": get_uid(),
                        "image": CONTAINER_JAVA_S3_COPY,
                        "command": 'copy_to_s3',
                        "user": 'root',
                        "min_frequency": frequency_pairs.bottom_frequency,
                        "max_frequency": frequency_pairs.top_frequency,
                        "additionalBindings": ['/home/ec2-user/.aws/credentials:/root/.aws/credentials'],
                    })
                    s3_drop_out = dropdict({
                        "type": 'plain',
                        "storage": 's3',
                        "oid": get_oid('s3'),
                        "uid": get_uid(),
                        "expireAfterUse": True,
                        "precious": False,
                        "bucket": args.bucket,
                        "key": 'split{3}/{0}_{1}/{2}.tar'.format(
                            frequency_pairs.bottom_frequency,
                            frequency_pairs.top_frequency,
                            get_observation(s3_drop['key']),
                            '' if args.width == 4 else '_' + args.width
                        ),
                        "profile_name": 'aws-chiles02',
                    })
                    copy_to_s3.addInput(result)
                    copy_to_s3.addOutput(s3_drop_out)

                    drop_list.append(copy_to_s3)
                    drop_list.append(s3_drop_out)
                    end_of_last_element = s3_drop_out

                outputs.append(end_of_last_element)

            barrier_drop = dropdict({
                "type": 'app',
                "app": get_module_name(BarrierAppDROP),
                "oid": get_oid('app'),
                "uid": get_uid(),
                "user": 'root',
                "input_error_threshold": 100,
            })
            drop_list.append(barrier_drop)

            for output in outputs:
                barrier_drop.addInput(output)

            full_drop_list.extend(drop_list)

    return full_drop_list, [sqlite_drop['uid']]


def command_json(args):
    drop_list, start_uids = build_graph(args)
    json_dumps = json.dumps(drop_list, indent=2, cls=SetEncoder)
    LOG.info(json_dumps)
    with open("/tmp/json.txt", "w") as json_file:
        json_file.write(json_dumps)


def command_run(args):
    drop_list, start_uids = build_graph(args)

    client = NodeManagerClient(args.host, args.port)

    session_id = get_session_id()
    client.create_session(session_id)
    client.append_graph(session_id, drop_list)
    client.deploy_session(session_id, start_uids)


def parser_arguments():
    parser = argparse.ArgumentParser('Build the MSTRANSFORM physical graph for a day')

    subparsers = parser.add_subparsers()

    parser_json = subparsers.add_parser('json', help='display the json')
    parser_json.add_argument('bucket', help='the bucket to access')
    parser_json.add_argument('ami_size', help='the ami size to sort the measurement sets')
    parser_json.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    parser_json.add_argument('cores', type=int, help='the number of cores')
    parser_json.add_argument('-w', '--width', type=int, help='the frequency width', default=4)
    parser_json.set_defaults(func=command_json)

    parser_run = subparsers.add_parser('run', help='run and deploy')
    parser_run.add_argument('bucket', help='the bucket to access')
    parser_run.add_argument('ami_size', help='the ami size to sort the measurement sets')
    parser_run.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    parser_run.add_argument('cores', type=int, help='the number of cores')
    parser_run.add_argument('host', help='the host the dfms is running on')
    parser_run.add_argument('-p', '--port', type=int, help='the port to bind to', default=8000)
    parser_run.add_argument('-w', '--width', type=int, help='the frequency width', default=4)
    parser_run.set_defaults(func=command_run)

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    arguments = parser_arguments()
    arguments.func(arguments)
