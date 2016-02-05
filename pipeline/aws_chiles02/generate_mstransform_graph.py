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

from aws_chiles02.common import get_oid, get_module_name, get_uid, get_session_id, get_observation, CONTAINER_JAVA_S3_COPY, CONTAINER_CHILES02, make_groups_of_frequencies, FREQUENCY_GROUPS
from aws_chiles02.apps import DockerCopyFromS3, DockerCopyToS3, DockerMsTransform, DockerListobs, CleanUpApp
from dfms.drop import DirectoryContainer, dropdict, BarrierAppDROP
from dfms.manager.client import NodeManagerClient, SetEncoder

LOG = logging.getLogger(__name__)
_1GB = 1073741824


class MeasurementSetData:
    def __init__(self, name, size):
        self.name = name
        self.size = size


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
                if key.size <= 200 * _1GB:
                    queue = 'xlarge'
                elif key.size <= 500 * _1GB:
                    queue = '2xlarge'
                else:
                    queue = '4xlarge'

                LOG.info('{0}, {1}, {2}'.format(key.key, key.size, queue))

                if queue == args.ami_size:
                    list_measurement_sets.append(MeasurementSetData(key.key, key.size))

    # Make sure the smallest is first
    sorted_list_measurement_sets = sorted(list_measurement_sets, key=operator.attrgetter('size'))

    number_in_chain = len(FREQUENCY_GROUPS) / args.cores
    full_drop_list = []
    first_drop = None
    clean_up_app = None

    for day_to_process in sorted_list_measurement_sets[:2]:
        drop_list = []
        s3_drop = dropdict({
            "type": 'plain',
            "storage": 's3',
            "oid": get_oid('s3'),
            "uid": get_uid(),
            "precious": False,
            "bucket": args.bucket,
            "key": day_to_process.name,
            "profile_name": 'aws-chiles02',
        })
        if first_drop is None:
            first_drop = s3_drop
        elif clean_up_app is not None:
            clean_up_app.addOutput(s3_drop)

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
            "clean_up": True,
        })

        copy_from_s3.addInput(s3_drop)
        copy_from_s3.addOutput(measurement_set)

        drop_list.append(s3_drop)
        drop_list.append(copy_from_s3)
        drop_list.append(measurement_set)

        casa_py_drop = dropdict({
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
            "clean_up": True,
        })

        casa_py_drop.addInput(measurement_set)
        casa_py_drop.addOutput(properties)

        drop_list.append(casa_py_drop)
        drop_list.append(properties)

        outputs = []
        for group in make_groups_of_frequencies(number_in_chain):
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
                    "min_frequency": frequency_pairs[0],
                    "max_frequency": frequency_pairs[1],
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
                    "clean_up": True,
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
                    "min_frequency": frequency_pairs[0],
                    "max_frequency": frequency_pairs[1],
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
                    "key": 'split/{0}_{1}/{2}.tar'.format(
                            frequency_pairs[0],
                            frequency_pairs[1],
                            get_observation(s3_drop['key'])
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
        })
        drop_list.append(barrier_drop)

        for output in outputs:
            barrier_drop.addInput(output)

        clean_up_app = dropdict({
            "type": 'app',
            "app": get_module_name(CleanUpApp),
            "oid": get_oid('app'),
            "uid": get_uid(),
            "user": 'root',
        })
        drop_list.append(clean_up_app)

        clean_up_app.addInput(barrier_drop)
        for drop in drop_list:
            if 'clean_up' in drop and drop['clean_up']:
                clean_up_app.addInput(drop)

        full_drop_list.extend(drop_list)

    return full_drop_list, [first_drop['uid']]


def command_json(args):
    drop_list, start_uids = build_graph(args)
    LOG.info(json.dumps(drop_list, indent=2, cls=SetEncoder))


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
    parser_json.set_defaults(func=command_json)

    parser_run = subparsers.add_parser('run', help='run and deploy')
    parser_run.add_argument('bucket', help='the bucket to access')
    parser_run.add_argument('ami_size', help='the ami size to sort the measurement sets')
    parser_run.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    parser_run.add_argument('cores', type=int, help='the number of cores')
    parser_run.add_argument('host', help='the host the dfms is running on')
    parser_run.add_argument('-p', '--port', type=int, help='the port to bind to', default=8000)
    parser_run.set_defaults(func=command_run)

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    arguments = parser_arguments()
    arguments.func(arguments)
