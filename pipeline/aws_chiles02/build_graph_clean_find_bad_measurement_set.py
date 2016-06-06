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

from aws_chiles02.apps_clean import DockerClean, CopyCleanFromS3
from aws_chiles02.common import get_module_name
from aws_chiles02.build_graph_common import AbstractBuildGraph
from aws_chiles02.settings_file import CONTAINER_CHILES02


class CarryOverDataClean:
    def __init__(self):
        self.s3_out = None
        self.clean_up = None


class BuildGraphCleanFindBadMeasurementSet(AbstractBuildGraph):
    def __init__(self, bucket_name, volume, parallel_streams, node_details, shutdown, width, bottom_frequency, session_id):
        super(BuildGraphCleanFindBadMeasurementSet, self).__init__(bucket_name, shutdown, node_details, volume, session_id)
        self._parallel_streams = parallel_streams
        self._s3_uvsub_name = 'uvsub_{0}'.format(width)
        self._min_frequency = bottom_frequency
        self._max_frequency = bottom_frequency + width
        values = node_details.values()
        self._node_id = values[0][0]['ip_address']
        self._s3_client = None

    def new_carry_over_data(self):
        return CarryOverDataClean()

    def build_graph(self):
        session = boto3.Session(profile_name='aws-chiles02')
        s3 = session.resource('s3', use_ssl=False)
        self._s3_client = s3.meta.client
        self._bucket = s3.Bucket(self._bucket_name)

        # Get all the cleaned files names
        s3_objects = []
        prefix = '{0}/{1}_{2}/'.format(
            self._s3_uvsub_name,
            self._min_frequency,
            self._max_frequency,
        )
        for key in self._bucket.objects.filter(Prefix=prefix):
            if key.key.endswith('.tar'):
                s3_objects.append(key.key)

        parallel_streams = [None] * self._parallel_streams
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
                get_module_name(CopyCleanFromS3),
                'app_copy_from_s3',
                min_frequency=self._min_frequency,
                max_frequency=self._max_frequency,
            )
            measurement_set = self.create_directory_container(
                self._node_id,
                'dir_in_ms'
            )

            # The order of arguments is important so don't put anything in front of these
            copy_from_s3.addInput(s3_drop)
            copy_from_s3.addOutput(measurement_set)

            self._start_oids.append(s3_drop['uid'])
            if parallel_streams[counter] is not None:
                copy_from_s3.addInput(parallel_streams[counter])

            casa_py_clean_drop = self.create_docker_app(
                self._node_id,
                get_module_name(DockerClean),
                'app_clean',
                CONTAINER_CHILES02,
                'clean',
                min_frequency=self._min_frequency,
                max_frequency=self._max_frequency,
                iterations=1,
                measurement_sets=[s3_drop],
            )
            elements = s3_object.split('/')
            result = self.create_directory_container(
                self._node_id,
                'dir_clean_output_{0}'.format(elements[2])
            )
            casa_py_clean_drop.addInput(measurement_set)
            casa_py_clean_drop.addOutput(result)
            parallel_streams[counter] = result

            counter += 1
            if counter >= self._parallel_streams:
                counter = 0

        self.copy_logfiles_and_shutdown()
