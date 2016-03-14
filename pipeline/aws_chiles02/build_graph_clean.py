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

from aws_chiles02.apps_clean import DockerClean, CopyCleanFromS3
from aws_chiles02.apps_general import CleanupDirectories
from aws_chiles02.common import get_module_name
from aws_chiles02.build_graph_common import AbstractBuildGraph
from aws_chiles02.settings_file import CONTAINER_CHILES02
from dfms.apps.bash_shell_app import BashShellApp
from dfms.drop import dropdict, DirectoryContainer


class CarryOverDataClean:
    def __init__(self):
        self.s3_out = None
        self.clean_up = None


class BuildGraphClean(AbstractBuildGraph):
    def __init__(self, work_to_do, bucket_name, volume, parallel_streams, node_details, shutdown, width, iterations):
        super(BuildGraphClean, self).__init__(bucket_name, shutdown, node_details, volume)
        self._work_to_do = work_to_do
        self._parallel_streams = parallel_streams
        self._s3_clean_name = 'clean_{0}_{1}'.format(width, iterations)
        self._s3_split_name = 'split_{0}'.format(width)
        self._iterations = iterations
        self._map_frequency_to_node = None
        self._list_ip = []
        self._s3_client = None

    def new_carry_over_data(self):
        return CarryOverDataClean()

    def build_graph(self):
        self._build_node_map()

        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)
        self._s3_client = s3.meta.client
        self._bucket = s3.Bucket(self._bucket_name)

        # Add the start drops
        for frequency_pair in self._work_to_do:
            node_id = self._get_next_node(frequency_pair)
            s3_drop_outs = self._build_s3_download(node_id, frequency_pair)

            casa_py_clean_drop = dropdict({
                "type": 'app',
                "app": get_module_name(DockerClean),
                "oid": self.get_oid('app_clean'),
                "uid": self.get_uuid(),
                "image": CONTAINER_CHILES02,
                "command": 'clean',
                "user": 'root',
                "min_frequency": frequency_pair.bottom_frequency,
                "max_frequency": frequency_pair.top_frequency,
                "iterations": self._iterations,
                "measurement_sets": [drop['dirname'] for drop in s3_drop_outs],
                "input_error_threshold": 100,
                "node": node_id,
            })
            oid = self.get_oid('dir_clean_output')
            result = dropdict({
                "type": 'container',
                "container": get_module_name(DirectoryContainer),
                "oid": oid,
                "uid": self.get_uuid(),
                "precious": False,
                "dirname": os.path.join(self._volume, oid),
                "check_exists": False,
                "expireAfterUse": True,
                "node": node_id,
            })
            for drop in s3_drop_outs:
                casa_py_clean_drop.addInput(drop)
            casa_py_clean_drop.addOutput(result)
            self.append(casa_py_clean_drop)
            self.append(result)

            copy_to_s3 = dropdict({
                "type": 'app',
                "app": get_module_name(CopyCleanToS3),
                "oid": self.get_oid('app_copy_clean_to_s3'),
                "uid": self.get_uuid(),
                "user": 'root',
                "min_frequency": frequency_pair.bottom_frequency,
                "max_frequency": frequency_pair.top_frequency,
                "input_error_threshold": 100,
                "node": node_id,
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
                "key": '{0}/cleaned_{1}_{2}.tar'.format(
                        self._s3_clean_name,
                        frequency_pair.bottom_frequency,
                        frequency_pair.top_frequency,
                ),
                "profile_name": 'aws-chiles02',
                "node": node_id,
            })
            copy_to_s3.addInput(result)
            copy_to_s3.addOutput(s3_drop_out)
            self.append(copy_to_s3)
            self.append(s3_drop_out)
            carry_over_data = self._map_carry_over_data[node_id]
            carry_over_data.s3_out = s3_drop_out

            clean_up = dropdict({
                "type": 'app',
                "app": get_module_name(CleanupDirectories),
                "oid": self.get_oid('app_cleanup_directories'),
                "uid": self.get_uuid(),
                "user": 'root',
                "node": node_id,
            })
            self.append(clean_up)
            for drop in s3_drop_outs:
                clean_up.addInput(drop)
            clean_up.addInput(s3_drop_out)
            carry_over_data.clean_up = clean_up

        if self._shutdown:
            self.add_shutdown()

    def add_shutdown(self):
        for node_id in self._list_ip:
            carry_over_data = self._map_carry_over_data[node_id]
            if carry_over_data.clean_up is not None:
                memory_drop = dropdict({
                    "type": 'plain',
                    "storage": 'memory',
                    "oid": self.get_oid('memory_in'),
                    "uid": self.get_uuid(),
                    "node": node_id,
                })
                self.append(memory_drop)
                carry_over_data.clean_up.addOutput(memory_drop)

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

    def _get_next_node(self, frequency_to_process):
        return self._map_frequency_to_node[frequency_to_process]

    def _build_node_map(self):
        self._list_ip = []
        for key, values in self._node_details.iteritems():
            for value in values:
                self._list_ip.append(value['ip_address'])

        self._map_frequency_to_node = {}
        count = 0
        for frequency_to_process in self._work_to_do:
            self._map_frequency_to_node[frequency_to_process] = self._list_ip[count]

            count += 1
            if count >= len(self._list_ip):
                count = 0

    def _build_s3_download(self, node_id, frequency_pair):
        s3_objects = []
        prefix = '{0}/{1}_{2}'.format(self._s3_split_name, frequency_pair.bottom_frequency, frequency_pair.top_frequency)
        for key in self._bucket.objects.filter(Prefix=prefix):
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
                "node": node_id,
            })
            copy_from_s3 = dropdict({
                "type": 'app',
                "app": get_module_name(CopyCleanFromS3),
                "oid": self.get_oid('app_copy_from_s3'),
                "uid": self.get_uuid(),
                "min_frequency": frequency_pair.bottom_frequency,
                "max_frequency": frequency_pair.top_frequency,
                "user": 'root',
                "input_error_threshold": 100,
                "node": node_id,
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
                "node": node_id,
            })
            # The order of arguments is important so don't put anything in front of these
            copy_from_s3.addInput(s3_drop)
            copy_from_s3.addOutput(measurement_set)

            self._start_oids.append(s3_drop['uid'])
            carry_over_data = self._map_carry_over_data[node_id]
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

        return s3_out_drops
