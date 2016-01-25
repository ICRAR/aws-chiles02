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
import logging
import os

from aws_chiles02.common import get_oid, get_module_name, get_uid, get_session_id, get_observation
from aws_chiles02.docker_apps import DockerCopyFromS3, DockerCopyToS3, DockerMsTransform
from dfms.drop import DirectoryContainer, dropdict, BarrierAppDROP
from dfms.manager.client import NodeManagerClient

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)


def build_graph(args):
    drop_list = []

    s3_drop = dropdict({
        "type": 'plain',
        "storage": 's3',
        "oid": get_oid('s3'),
        "uid": get_uid(),
        "bucket": args.bucket,
        "key": args.ms_set,
        "aws_access_key_id": args.aws_access_key_id,
        "aws_secret_access_key": args.aws_secret_access_key
    })
    copy_from_s3 = dropdict({
        "type": 'app',
        "app": get_module_name(DockerCopyFromS3),
        "oid": get_oid('app'),
        "uid": get_uid(),
        "aws_access_key_id": args.aws_access_key_id,
        "aws_secret_access_key": args.aws_secret_access_key,
        "user": 'root'
    })
    oid = get_oid('dir')
    measurement_set = dropdict({
        "type": 'container',
        "container": get_module_name(DirectoryContainer),
        "oid": oid,
        "uid": get_uid(),
        "dirname": os.path.join(args.volume, oid),
        "check_exists": False
    })

    copy_from_s3.addInput(s3_drop)
    copy_from_s3.addOutput(measurement_set)

    drop_list.append(s3_drop)
    drop_list.append(copy_from_s3)
    drop_list.append(measurement_set)

    outputs = []
    for frequency_pairs in [[980, 984], [984, 988], [988, 992]]:
        casa_py_drop = dropdict({
            "type": 'app',
            "app": get_module_name(DockerMsTransform),
            "oid": get_oid('app'),
            "uid": get_uid(),
            "min_frequency": frequency_pairs[0],
            "max_frequency": frequency_pairs[1],
            "user": 'root'
        })
        oid = get_oid('dir')
        result = dropdict({
            "type": 'container',
            "container": get_module_name(DirectoryContainer),
            "oid": oid,
            "uid": get_uid(),
            "dirname": os.path.join(args.volume, oid),
            "check_exists": False
        })

        casa_py_drop.addInput(measurement_set)
        casa_py_drop.addOutput(result)

        drop_list.append(casa_py_drop)
        drop_list.append(result)

        copy_to_s3 = dropdict({
            "type": 'app',
            "app": get_module_name(DockerCopyToS3),
            "oid": get_oid('app'),
            "uid": get_uid(),
            "user": 'root'
        })
        s3_drop_out = dropdict({
            "type": 'plain',
            "storage": 's3',
            "oid": get_oid('s3'),
            "uid": get_uid(),
            "bucket": args.bucket,
            "key": '{0}_{1}/{2}'.format(
                    frequency_pairs[0],
                    frequency_pairs[1],
                    get_observation(s3_drop)
            ),
            "aws_access_key_id": args.aws_access_key_id,
            "aws_secret_access_key": args.aws_secret_access_key

        })
        drop_list.append(copy_to_s3)
        drop_list.append(s3_drop_out)
        outputs.append(s3_drop_out)

    barrier_drop = dropdict({
        "type": 'app',
        "app": get_module_name(BarrierAppDROP),
        "oid": get_oid('app'),
        "uid": get_uid(),
        "user": 'root'
    })
    drop_list.append(barrier_drop)
    barrier_drop.addInput(measurement_set)

    for output in outputs:
        barrier_drop.addInput(output)

    client = NodeManagerClient(args.host, int(args.port))

    session_id = get_session_id()
    client.create_session(session_id)
    client.append_graph(session_id, drop_list)
    client.deploy_session(session_id, [s3_drop['uid']])


def parser_arguments():
    parser = argparse.ArgumentParser('Build the physical graph for a day')
    parser.add_argument('aws_access_key_id', help="the AWS aws_access_key_id to use")
    parser.add_argument('aws_secret_access_key', help="the AWS aws_secret_access_key to use")
    parser.add_argument('bucket', help='the bucket to access')
    parser.add_argument('ms_set', help='the measurement set key')
    parser.add_argument('volume', help='the directory on the host to bind to the Docker Apps')
    parser.add_argument('host', help='the host the dfms is running on')
    parser.add_argument('-p', '--port', help='the port to bind to', default=8000)

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    arguments = parser_arguments()
    build_graph(arguments)
