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
Build the physical graph
"""
import os

import boto3

from aws_chiles02.apps_jpeg2000 import CopyFitsFromS3, CopyJpeg2000ToS3
from aws_chiles02.common import get_module_name
from aws_chiles02.build_graph_common import AbstractBuildGraph
from aws_chiles02.settings_file import CONTAINER_SV
from dfms.apps.bash_shell_app import BashShellApp
from dfms.apps.dockerapp import DockerApp
from dfms.drop import dropdict, FileDROP, BarrierAppDROP


class CarryOverDataJpeg2000:
    def __init__(self):
        self.barrier_drop = None


class BuildGraphJpeg2000(AbstractBuildGraph):
    def __init__(self, bucket_name, volume, parallel_streams, node_details, shutdown, width, iterations):
        super(BuildGraphJpeg2000, self).__init__(bucket_name, shutdown, node_details, volume)
        self._parallel_streams = parallel_streams
        self._s3_fits_name = 'fits_{0}_{1}'.format(width, iterations)
        self._s3_jpeg2000_name = 'jpeg_{0}_{1}'.format(width, iterations)
        self._iterations = iterations
        values = node_details.values()
        self._node_id = values[0][0]['ip_address']
        self._width = width
        self._s3_client = None

    def new_carry_over_data(self):
        return CarryOverDataJpeg2000()

    def build_graph(self):
        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)
        self._s3_client = s3.meta.client
        self._bucket = s3.Bucket(self._bucket_name)

        # Get the ones we've already done
        already_done = []
        prefix = '{0}/'.format(self._s3_jpeg2000_name)
        for key in self._bucket.objects.filter(Prefix=prefix):
            if key.key.endswith('.jpx'):
                (head, tail) = os.path.split(key.key)
                (name, ext) = os.path.splitext(tail)
                already_done.append(name[6:])

        # Add the cleaned images
        s3_objects = []
        prefix = '{0}/'.format(self._s3_fits_name)
        for key in self._bucket.objects.filter(Prefix=prefix):
            if key.key.endswith('.fits'):
                (head, tail) = os.path.split(key.key)
                (name, ext) = os.path.splitext(tail)
                if name[8:] not in already_done:
                    s3_objects.append(key.key)

        parallel_streams = [None] * self._parallel_streams
        counter = 0
        for s3_object in s3_objects:
            s3_drop = dropdict({
                "type": 'plain',
                "storage": 's3',
                "oid": self.get_oid('s3_in'),
                "uid": self.get_uuid(),
                "precious": False,
                "bucket": self._bucket_name,
                "key": s3_object,
                "profile_name": 'aws-chiles02',
                "node": self._node_id,
            })
            copy_from_s3 = dropdict({
                "type": 'app',
                "app": get_module_name(CopyFitsFromS3),
                "oid": self.get_oid('app_copy_from_s3'),
                "uid": self.get_uuid(),
                "input_error_threshold": 100,
                "node": self._node_id,
            })

            (minimum_frequency, maximum_frequency) = self._get_frequencies(s3_object)
            fits_file_name = self._get_fits_file_name(s3_object)
            fits_file = dropdict({
                "type": 'plain',
                "storage": 'file',
                "oid": self.get_oid('fits_file'),
                "uid": self.get_uuid(),
                "precious": False,
                "filepath": os.path.join(self._volume, fits_file_name),
                "node": self._node_id,
            })
            # The order of arguments is important so don't put anything in front of these
            copy_from_s3.addInput(s3_drop)
            copy_from_s3.addOutput(fits_file)

            self._start_oids.append(s3_drop['uid'])
            if parallel_streams[counter] is not None:
                copy_from_s3.addInput(parallel_streams[counter])

            self.append(s3_drop)
            self.append(copy_from_s3)
            self.append(fits_file)

            # Do the conversions
            convert_jpeg2000 = dropdict({
                "type": 'app',
                "app": get_module_name(DockerApp),
                "oid": self.get_oid('app_convert_jpeg2000'),
                "uid": self.get_uuid(),
                "image": CONTAINER_SV,
                "command": 'sv-encode -i %i0 -o %o0 Clayers=15 Clevels=6 Cycc=no Corder=CPRL ORGgen_plt=yes Cprecincts="{256,256},{128,128}" Cblk="{32,32}" Qstep=0.0001',
                "user": 'root',
                "node": self._node_id,
            })

            jpeg2000_name = self._get_jpeg2000_name(s3_object)
            jpeg2000_file = dropdict({
                "type": 'plain',
                "storage": 'file',
                "container": get_module_name(FileDROP),
                "oid": self.get_oid('fits_file'),
                "uid": self.get_uuid(),
                "precious": False,
                "filepath": os.path.join(self._volume, jpeg2000_name),
                "node": self._node_id,
            })

            convert_jpeg2000.addInput(fits_file)
            convert_jpeg2000.addOutput(jpeg2000_file)
            self.append(convert_jpeg2000)
            self.append(jpeg2000_file)

            copy_jpg2000_to_s3 = dropdict({
                "type": 'app',
                "app": get_module_name(CopyJpeg2000ToS3),
                "oid": self.get_oid('app_copy_jpeg_to_s3'),
                "uid": self.get_uuid(),
                "input_error_threshold": 100,
                "node": self._node_id,
            })
            s3_jpeg2000_drop_out = dropdict({
                "type": 'plain',
                "storage": 's3',
                "oid": self.get_oid('s3_out'),
                "uid": self.get_uuid(),
                "expireAfterUse": True,
                "precious": False,
                "bucket": self._bucket_name,
                "key": '{0}/image_{1}_{2}.jpx'.format(
                    self._s3_jpeg2000_name,
                    minimum_frequency,
                    maximum_frequency,
                ),
                "profile_name": 'aws-chiles02',
                "node": self._node_id,
            })
            copy_jpg2000_to_s3.addInput(jpeg2000_file)
            copy_jpg2000_to_s3.addOutput(s3_jpeg2000_drop_out)
            self.append(copy_jpg2000_to_s3)
            self.append(s3_jpeg2000_drop_out)

            parallel_streams[counter] = s3_jpeg2000_drop_out

            counter += 1
            if counter >= self._parallel_streams:
                counter = 0

        barrier_drop = dropdict({
            "type": 'app',
            "app": get_module_name(BarrierAppDROP),
            "oid": self.get_oid('app_barrier'),
            "uid": self.get_uuid(),
            "input_error_threshold": 100,
            "node": self._node_id,
        })
        self.append(barrier_drop)
        for jpeg2000_file in parallel_streams:
            barrier_drop.addInput(jpeg2000_file)
        carry_over_data = self._map_carry_over_data[self._node_id]
        carry_over_data.barrier_drop = barrier_drop

        if self._shutdown:
            self.add_shutdown()

    def add_shutdown(self):
        for list_ips in self._node_details.values():
            for instance_details in list_ips:
                node_id = instance_details['ip_address']
                carry_over_data = self._map_carry_over_data[node_id]
                if carry_over_data.barrier_drop is not None:
                    memory_drop = dropdict({
                        "type": 'plain',
                        "storage": 'memory',
                        "oid": self.get_oid('memory_in'),
                        "uid": self.get_uuid(),
                        "node": node_id,
                    })
                    self.append(memory_drop)
                    carry_over_data.barrier_drop.addOutput(memory_drop)

                    shutdown_drop = dropdict({
                        "type": 'app',
                        "app": get_module_name(BashShellApp),
                        "oid": self.get_oid('app_bash_shell_app'),
                        "uid": self.get_uuid(),
                        "command": 'sudo shutdown -h +5 "DFMS node shutting down" &',
                        "user": 'root',
                        "input_error_threshold": 100,
                        "node": node_id,
                    })
                    shutdown_drop.addInput(memory_drop)
                    self.append(shutdown_drop)

    @staticmethod
    def _get_fits_file_name(s3_object):
        (head, tail) = os.path.split(s3_object)
        return tail

    @staticmethod
    def _get_jpeg2000_name(s3_object):
        (head, tail) = os.path.split(s3_object)
        (name, ext) = os.path.splitext(tail)
        return name + '.jpx'

    @staticmethod
    def _get_frequencies(s3_object):
        (head, tail) = os.path.split(s3_object)
        (name, ext) = os.path.splitext(tail)
        elements = name.split('_')
        return elements[1], elements[2]
