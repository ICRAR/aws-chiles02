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

from aws_chiles02.apps_concatenate import CopyConcatenateFromS3, CopyConcatenateToS3, DockerImageconcat
from aws_chiles02.common import get_module_name
from aws_chiles02.build_graph_common import AbstractBuildGraph
from aws_chiles02.settings_file import CONTAINER_CHILES02


class CarryOverDataConcatenation:
    def __init__(self):
        self.s3_out = None
        self.copy_to_s3 = None


class BuildGraphConcatenation(AbstractBuildGraph):
    def __init__(self, bucket_name, volume, parallel_streams, node_details, shutdown, width, iterations, session_id, dim_ip):
        super(BuildGraphConcatenation, self).__init__(bucket_name, shutdown, node_details, volume, session_id, dim_ip)
        self._parallel_streams = parallel_streams
        self._s3_image_name = 'final_image_{0}_{1}'.format(width, iterations)
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
            s3_drop = self.create_s3_drop(
                self._node_id,
                self._bucket_name,
                s3_object,
                'aws-chiles02',
                oid='s3_in',
            )
            copy_from_s3 = self.create_app(
                self._node_id,
                get_module_name(CopyConcatenateFromS3),
                'app_copy_from_s3',
            )

            measurement_set = self.create_directory_container(
                self._node_id,
                'dir_in_ms',
            )
            # The order of arguments is important so don't put anything in front of these
            copy_from_s3.addInput(s3_drop)
            copy_from_s3.addOutput(measurement_set)

            self._start_oids.append(s3_drop['uid'])
            carry_over_data = self._map_carry_over_data[self._node_id]
            if carry_over_data.s3_out is not None:
                copy_from_s3.addInput(carry_over_data.s3_out)

            if parallel_streams[counter] is not None:
                copy_from_s3.addInput(parallel_streams[counter])

            parallel_streams[counter] = measurement_set
            s3_out_drops.append(measurement_set)

            counter += 1
            if counter >= self._parallel_streams:
                counter = 0

        casa_py_concatenation_drop = self.create_docker_app(
            self._node_id,
            get_module_name(DockerImageconcat),
            'app_concatenate',
            CONTAINER_CHILES02,
            'concatenate',
            measurement_sets=[drop['dirname'] for drop in s3_out_drops],
            width=self._width,
            iterations=self._iterations,
        )

        result = self.create_directory_container(self._node_id, 'dir_concatenate_output')
        for drop in s3_out_drops:
            casa_py_concatenation_drop.addInput(drop)
        casa_py_concatenation_drop.addOutput(result)

        copy_to_s3 = self.create_app(
            self._node_id,
            get_module_name(CopyConcatenateToS3),
            'app_copy_concatenate_to_s3',
            width=self._width,
            iterations=self._iterations,
        )
        s3_drop_out = self.create_s3_drop(
            self._node_id,
            self._bucket_name,
            '{0}/image_{1}_{2}.tar'.format(
                self._s3_image_name,
                self._width,
                self._iterations,
            ),
            'aws-chiles02',
            oid='s3_out'
        )
        copy_to_s3.addInput(result)
        copy_to_s3.addOutput(s3_drop_out)
        carry_over_data = self._map_carry_over_data[self._node_id]
        carry_over_data.copy_to_s3 = copy_to_s3

        self.copy_logfiles_and_shutdown()
