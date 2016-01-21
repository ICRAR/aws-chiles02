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
Run the corner turning for a day
"""
import uuid

import argparse

from common import get_oid, make_groups_of_frequencies
from s3_drop import S3DROP
from dfms.apps.dockerapp import DockerApp
from dfms.drop import FileDROP, BarrierAppDROP


def build_graph(args):
    s3_drop = S3DROP(
            get_oid('s3'),
            uuid.uuid4(),
            bucket=args.bucket,
            key=args.ms_set,
            profile_name=args.profile)
    copy_from_s3 = DockerApp(
            get_oid('app'),
            uuid.uuid4(),
            image='java-s3-copy:latest',
            command='copy_from_s3.sh %iDataURL0 %o0 {0} {1}'.format(args.aws_access_key_id, args.aws_secret_access_key),
            user='root')
    measurement_set = FileDROP(get_oid('file'), uuid.uuid4(), dirname=args.volume)

    copy_from_s3.addInput(s3_drop)
    copy_from_s3.addOutput(measurement_set)

    outputs = []
    for group in make_groups_of_frequencies(8):
        first = True
        end_of_last_element = None
        for frequency_pairs in group:
            casa_py_drop = DockerApp(get_oid('app'), uuid.uuid4(), image='mock:latest', command='casa_py.sh %i0 %o0 {0} {1}'.format(frequency_pairs[0], frequency_pairs[1]), user='root')
            result = FileDROP(get_oid('file'), uuid.uuid4(), dirname=args.volume)
            copy_to_s3 = DockerApp(get_oid('app'), uuid.uuid4(), image='mock:latest', command='copy_to_s3.sh %i0 %oDataURL0', user='root')
            s3_drop_out = S3DROP(get_oid('s3'), uuid.uuid4(), bucket='mock', key='{0}_{1}/key123'.format(frequency_pairs[0], frequency_pairs[1]), profile_name='aws-profile')

            casa_py_drop.addInput(measurement_set)
            if first:
                first = False
            else:
                casa_py_drop.addInput(end_of_last_element)
            casa_py_drop.addOutput(result)
            copy_to_s3.addInput(result)
            copy_to_s3.addOutput(s3_drop_out)
            end_of_last_element = s3_drop_out

        outputs.append(end_of_last_element)

    barrier_drop = BarrierAppDROP(get_oid('barrier'), uuid.uuid4())
    barrier_drop.addInput(measurement_set)

    for output in outputs:
        barrier_drop.addInput(output)

    return barrier_drop, s3_drop


def parser_arguments():
    parser = argparse.ArgumentParser('Build the physical graph for a day')
    parser.add_argument('aws_access_key_id', help="the AWS aws_access_key_id to use")
    parser.add_argument('aws_secret_access_key', help="the AWS aws_secret_access_key to use")
    parser.add_argument('bucket', help='the bucket to access')
    parser.add_argument('ms_set', help='the measurement set key')
    parser.add_argument('volume', help='the directory on the host to bind to the Docker Apps')

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    arguments = parser_arguments()
    build_graph(arguments)
