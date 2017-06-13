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
from aws_chiles02.apps_general import CleanupDirectories
from aws_chiles02.apps_imageconcat import CopyFitsToS3, CopyImageconcatFromS3, CopyImageconcatToS3, DockerImageconcat
from aws_chiles02.build_graph_common import AbstractBuildGraph
from aws_chiles02.common import get_module_name
from aws_chiles02.settings_file import CONTAINER_CHILES02


class CarryOverDataImageconcat:
    def __init__(self):
        self.s3_out = None
        self.clean_up = None


class BuildGraphImageconcat(AbstractBuildGraph):
    def __init__(self, **keywords):
        super(BuildGraphImageconcat, self).__init__(**keywords)
        self._work_to_do = keywords['work_to_do']
        self._parallel_streams = keywords['parallel_streams']
        self._clean_directory_name = keywords['clean_directory_name']
        self._fits_directory_name = keywords['fits_directory_name']
        self._imageconcat_directory_name = keywords['imageconcat_directory_name']
        self._cleaned_objects = keywords['cleaned_objects']

    def new_carry_over_data(self):
        return CarryOverDataImageconcat()

    def build_graph(self):
        self._build_node_map()

        for frequency_pair in self._work_to_do:
            node_id = self._get_next_node(frequency_pair)
            s3_drop_outs = self._build_s3_download(node_id, frequency_pair)

            casa_imageconcat_drop = self.create_docker_app(
                node_id,
                get_module_name(DockerImageconcat),
                'app_imageconcat',
                CONTAINER_CHILES02,
                'imageconcat',
                min_frequency=frequency_pair.bottom_frequency,
                max_frequency=frequency_pair.top_frequency,
                measurement_sets=[drop['dirname'] for drop in s3_drop_outs],
            )
            result = self.create_directory_container(node_id, 'dir_imageconcat_output')
            for drop in s3_drop_outs:
                casa_imageconcat_drop.addInput(drop)
            casa_imageconcat_drop.addOutput(result)

            copy_imageconcat_to_s3 = self.create_app(
                node_id,
                get_module_name(CopyImageconcatToS3),
                'app_copy_imageconcat_to_s3',
                min_frequency=frequency_pair.bottom_frequency,
                max_frequency=frequency_pair.top_frequency,
            )
            s3_imageconcat_drop_out = self.create_s3_drop(
                node_id,
                self._bucket_name,
                '{0}/image_{1}_{2}.tar'.format(
                    self._imageconcat_directory_name,
                    frequency_pair.bottom_frequency,
                    frequency_pair.top_frequency,
                ),
                'aws-chiles02',
                oid='s3_out',
            )
            copy_imageconcat_to_s3.addInput(result)
            copy_imageconcat_to_s3.addOutput(s3_imageconcat_drop_out)

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
                '{0}/image_{1}_{2}.fits'.format(
                    self._fits_directory_name,
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
            barrier_drop.addInput(s3_imageconcat_drop_out)
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

        self.copy_parameter_data(self._imageconcat_directory_name)
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
        s3_objects = frequency_pair.pairs
        parallel_streams = [None] * self._parallel_streams
        s3_out_drops = []
        counter = 0
        for s3_object in s3_objects:
            expected_file = '{0}/cleaned_{1}_{2}.tar.centre'.format(
                self._clean_directory_name,
                s3_object.bottom_frequency,
                s3_object.top_frequency,
            )
            if expected_file in self._cleaned_objects:
                s3_drop = self.create_s3_drop(
                    node_id,
                    self._bucket_name,
                    expected_file,
                    'aws-chiles02',
                    oid='s3_in',
                )
                copy_from_s3 = self.create_app(
                    node_id,
                    get_module_name(CopyImageconcatFromS3),
                    'app_copy_from_s3',
                    min_frequency=s3_object.bottom_frequency,
                    max_frequency=s3_object.top_frequency,
                    clean_directory_name=self._clean_directory_name,
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
