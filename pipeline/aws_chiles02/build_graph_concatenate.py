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

from aws_chiles02.apps_concatenate import CopyConcatenateFromS3, CopyConcatenateToS3, DockerConcatenate
from aws_chiles02.common import get_module_name
from aws_chiles02.build_graph_common import AbstractBuildGraph
from aws_chiles02.settings_file import CONTAINER_CHILES02
from dfms.apps.bash_shell_app import BashShellApp
from dfms.drop import dropdict, DirectoryContainer


class CarryOverDataConcatenation:
    def __init__(self):
        self.s3_out = None
        self.copy_to_s3 = None


class BuildGraphConcatenation(AbstractBuildGraph):
    def __init__(self, bucket_name, volume, parallel_streams, node_details, shutdown, width, iterations):
        super(BuildGraphConcatenation, self).__init__(bucket_name, shutdown, node_details, volume)
        self._parallel_streams = parallel_streams
        self._s3_image_name = 'image_{0}_{1}'.format(width, iterations)
        self._s3_clean_name = 'clean_{0}_{1}'.format(width, iterations)
        self._iterations = iterations
        values = node_details.values()
        self._node_id = values[0][0]['ip_address']
        self._width = width
        self._s3_client = None

    def new_carry_over_data(self):
        return CarryOverDataConcatenation()

    def build_graph(self):
        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)
        self._s3_client = s3.meta.client
        self._bucket = s3.Bucket(self._bucket_name)

        # Add the cleaned images
        s3_objects = []
        prefix = '{0}/'.format(self._s3_clean_name)
        for key in self._bucket.objects.filter(Prefix=prefix):
            if key.key.endswith('.tar'):
                s3_objects.append(key.key)

        parallel_streams = [None] * self._parallel_streams
        s3_out_drops = []
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
                "app": get_module_name(CopyConcatenateFromS3),
                "oid": self.get_oid('app_copy_from_s3'),
                "uid": self.get_uuid(),
                "user": 'root',
                "input_error_threshold": 100,
                "node": self._node_id,
                "n_tries": 2,
            })

            oid01 = self.get_oid('dir_in_ms')
            measurement_set = dropdict({
                "type": 'container',
                "container": get_module_name(DirectoryContainer),
                "oid": oid01,
                "uid": self.get_uuid(),
                "precious": False,
                "dirname": os.path.join(self._volume, oid01),
                "check_exists": False,
                "node": self._node_id,
            })
            # The order of arguments is important so don't put anything in front of these
            copy_from_s3.addInput(s3_drop)
            copy_from_s3.addOutput(measurement_set)

            self._start_oids.append(s3_drop['uid'])
            carry_over_data = self._map_carry_over_data[self._node_id]
            if carry_over_data.s3_out is not None:
                copy_from_s3.addInput(carry_over_data.s3_out)

            if parallel_streams[counter] is not None:
                copy_from_s3.addInput(parallel_streams[counter])

            self.append(s3_drop)
            self.append(copy_from_s3)
            self.append(measurement_set)

            parallel_streams[counter] = measurement_set
            s3_out_drops.append(measurement_set)

            counter += 1
            if counter >= self._parallel_streams:
                counter = 0

        casa_py_concatenation_drop = dropdict({
            "type": 'app',
            "app": get_module_name(DockerConcatenate),
            "oid": self.get_oid('app_concatenate'),
            "uid": self.get_uuid(),
            "image": CONTAINER_CHILES02,
            "command": 'clean',
            "user": 'root',
            "measurement_sets": [drop['dirname'] for drop in s3_out_drops],
            "width": self._width,
            "iterations": self._iterations,
            "input_error_threshold": 100,
            "node": self._node_id,
            "n_tries": 2,
        })
        oid = self.get_oid('dir_concatenate_output')
        result = dropdict({
            "type": 'container',
            "container": get_module_name(DirectoryContainer),
            "oid": oid,
            "uid": self.get_uuid(),
            "precious": False,
            "dirname": os.path.join(self._volume, oid),
            "check_exists": False,
            "expireAfterUse": True,
            "node": self._node_id,
        })
        for drop in s3_out_drops:
            casa_py_concatenation_drop.addInput(drop)
        casa_py_concatenation_drop.addOutput(result)
        self.append(casa_py_concatenation_drop)
        self.append(result)

        copy_to_s3 = dropdict({
            "type": 'app',
            "app": get_module_name(CopyConcatenateToS3),
            "oid": self.get_oid('app_copy_concatenate_to_s3'),
            "uid": self.get_uuid(),
            "user": 'root',
            "width": self._width,
            "iterations": self._iterations,
            "node": self._node_id,
            "n_tries": 2,
        })
        s3_drop_out = dropdict({
            "type": 'plain',
            "storage": 's3',
            "oid": self.get_oid('s3_out'),
            "uid": self.get_uuid(),
            "expireAfterUse": True,
            "precious": False,
            "bucket": self._bucket_name,
            "key": '{0}/image_{1}_{2}.tar'.format(
                    self._s3_image_name,
                    self._width,
                    self._iterations,
            ),
            "profile_name": 'aws-chiles02',
            "node": self._node_id,
        })
        copy_to_s3.addInput(result)
        copy_to_s3.addOutput(s3_drop_out)
        self.append(copy_to_s3)
        self.append(s3_drop_out)
        carry_over_data = self._map_carry_over_data[self._node_id]
        carry_over_data.copy_to_s3 = copy_to_s3

        if self._shutdown:
            self.add_shutdown()

    def add_shutdown(self):
        for list_ips in self._node_details.values():
            for instance_details in list_ips:
                node_id = instance_details['ip_address']
                carry_over_data = self._map_carry_over_data[node_id]
                if carry_over_data.copy_to_s3 is not None:
                    memory_drop = dropdict({
                        "type": 'plain',
                        "storage": 'memory',
                        "oid": self.get_oid('memory_in'),
                        "uid": self.get_uuid(),
                        "node": node_id,
                    })
                    self.append(memory_drop)
                    carry_over_data.copy_to_s3.addOutput(memory_drop)

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
