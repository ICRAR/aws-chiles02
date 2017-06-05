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
import boto3

from .apps_clean import CopyCleanFromS3, CopyCleanToS3, CopyFitsToS3, DockerClean
from .apps_general import CleanupDirectories
from .apps_tclean import DockerTclean
from .build_graph_common import AbstractBuildGraph
from .common import get_module_name
from .settings_file import CONTAINER_CHILES02


class CarryOverDataClean:
    def __init__(self):
        self.s3_out = None
        self.clean_up = None


class BuildGraphClean(AbstractBuildGraph):
    def __init__(self, **keywords):
        super(BuildGraphClean, self).__init__(**keywords)
        self._work_to_do = keywords['work_to_do']
        self._parallel_streams = keywords['parallel_streams']
        self._s3_clean_name = keywords['clean_directory_name']
        self._s3_fits_name = keywords['fits_directory_name']
        self._s3_uvsub_name = keywords['uvsub_directory_name']
        self._iterations = keywords['iterations']
        self._arcsec = keywords['arcsec']
        self._w_projection_planes = keywords['w_projection_planes']
        self._robust = keywords['robust']
        self._image_size = keywords['image_size']
        self._clean_channel_average = keywords['clean_channel_average']
        self._only_image = keywords['only_image']
        self._produce_qa = keywords['produce_qa']
        self._clean_tclean = keywords['clean_tclean']
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

            casa_py_clean_drop = self.create_docker_app(
                node_id,
                get_module_name(DockerClean) if self._clean_tclean == 'clean' else get_module_name(DockerTclean),
                'app_clean' if self._clean_tclean == 'clean' else 'app_tclean',
                CONTAINER_CHILES02,
                'clean' if self._clean_tclean == 'clean' else 'tclean',
                min_frequency=frequency_pair.bottom_frequency,
                max_frequency=frequency_pair.top_frequency,
                iterations=self._iterations,
                arcsec=self._arcsec,
                robust=self._robust,
                image_size=self._image_size,
                w_projection_planes=self._w_projection_planes,
                clean_channel_average=self._clean_channel_average,
                produce_qa=self._produce_qa,
                measurement_sets=[drop['dirname'] for drop in s3_drop_outs],
            )
            result = self.create_directory_container(node_id, 'dir_clean_output')
            for drop in s3_drop_outs:
                casa_py_clean_drop.addInput(drop)
            casa_py_clean_drop.addOutput(result)

            copy_clean_to_s3 = self.create_app(
                node_id,
                get_module_name(CopyCleanToS3),
                'app_copy_clean_to_s3',
                min_frequency=frequency_pair.bottom_frequency,
                max_frequency=frequency_pair.top_frequency,
                only_image=self._only_image,
            )
            s3_clean_drop_out = self.create_s3_drop(
                node_id,
                self._bucket_name,
                '{0}/cleaned_{1}_{2}.tar'.format(
                    self._s3_clean_name,
                    frequency_pair.bottom_frequency,
                    frequency_pair.top_frequency,
                ),
                'aws-chiles02',
                oid='s3_out',
            )
            copy_clean_to_s3.addInput(result)
            copy_clean_to_s3.addOutput(s3_clean_drop_out)

            copy_fits_to_s3 = self.create_app(
                node_id,
                get_module_name(CopyFitsToS3),
                'app_copy_fits_to_s3',
                min_frequency=frequency_pair.bottom_frequency,
                max_frequency=frequency_pair.top_frequency,
            )
            s3_fits_drop_out = self.create_s3_drop(
                node_id,
                self._bucket_name,
                '{0}/cleaned_{1}_{2}.fits'.format(
                    self._s3_fits_name,
                    frequency_pair.bottom_frequency,
                    frequency_pair.top_frequency,
                ),
                'aws-chiles02',
                oid='s3_out',
            )
            copy_fits_to_s3.addInput(result)
            copy_fits_to_s3.addOutput(s3_fits_drop_out)

            barrier_drop = self.create_barrier_app(node_id)

            # Give the memory drop somewhere to go
            memory_drop = self.create_memory_drop(node_id)
            barrier_drop.addInput(s3_clean_drop_out)
            barrier_drop.addInput(s3_fits_drop_out)
            barrier_drop.addOutput(memory_drop)

            carry_over_data = self._map_carry_over_data[node_id]
            carry_over_data.s3_out = memory_drop

            clean_up = self.create_app(
                node_id,
                get_module_name(CleanupDirectories),
                'app_cleanup_directories',
            )
            for drop in s3_drop_outs:
                clean_up.addInput(drop)
            clean_up.addInput(result)
            clean_up.addInput(memory_drop)
            carry_over_data.clean_up = clean_up

        self.copy_parameter_data(self._s3_clean_name)
        self.copy_logfiles_and_shutdown()

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
        prefix = '{0}/{1}_{2}'.format(self._s3_uvsub_name, frequency_pair.bottom_frequency, frequency_pair.top_frequency)
        for key in self._bucket.objects.filter(Prefix=prefix):
            if not key.key.startswith('stats'):
                s3_objects.append(key.key)

        parallel_streams = [None] * self._parallel_streams
        s3_out_drops = []
        counter = 0
        for s3_object in s3_objects:
            s3_drop = self.create_s3_drop(
                node_id,
                self._bucket_name,
                s3_object,
                'aws-chiles02',
                oid='s3_in',
            )
            copy_from_s3 = self.create_app(
                node_id,
                get_module_name(CopyCleanFromS3),
                'app_copy_from_s3',
                min_frequency=frequency_pair.bottom_frequency,
                max_frequency=frequency_pair.top_frequency,

            )
            measurement_set = self.create_directory_container(
                node_id,
                'dir_in_ms'
            )

            # The order of arguments is important so don't put anything in front of these
            copy_from_s3.addInput(s3_drop)
            copy_from_s3.addOutput(measurement_set)

            carry_over_data = self._map_carry_over_data[node_id]
            if carry_over_data.s3_out is not None:
                copy_from_s3.addInput(carry_over_data.s3_out)

            if parallel_streams[counter] is not None:
                copy_from_s3.addInput(parallel_streams[counter])

            parallel_streams[counter] = measurement_set
            s3_out_drops.append(measurement_set)

            counter += 1
            if counter >= self._parallel_streams:
                counter = 0

        return s3_out_drops
