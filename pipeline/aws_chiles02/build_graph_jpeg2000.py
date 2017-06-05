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
import os

from .apps_jpeg2000 import CopyFitsFromS3, CopyJpeg2000ToS3
from .build_graph_common import AbstractBuildGraph
from .common import get_module_name
from .settings_file import CONTAINER_SV
from dfms.apps.dockerapp import DockerApp
from dfms.drop import BarrierAppDROP


class CarryOverDataJpeg2000:
    def __init__(self):
        self.barrier_drop = None


class BuildGraphJpeg2000(AbstractBuildGraph):
    def __init__(self, **kwargs):
        super(BuildGraphJpeg2000, self).__init__(**kwargs)
        self._parallel_streams = kwargs['parallel_streams']
        self._s3_fits_name = kwargs['fits_directory_name']
        self._s3_jpeg2000_name = kwargs['jpeg2000_directory_name']
        values = kwargs['node_details'].values()
        self._node_id = values[0][0]['ip_address']
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

        # Add the fits
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
            s3_drop = self.create_s3_drop(
                self._node_id,
                self._bucket_name,
                s3_object,
                'aws-chiles02',
                oid='s3_in',
            )
            copy_from_s3 = self.create_app(
                self._node_id,
                get_module_name(CopyFitsFromS3),
                'app_copy_from_s3',
            )

            (minimum_frequency, maximum_frequency) = self._get_frequencies(s3_object)
            fits_file_name = self._get_fits_file_name(s3_object)
            fits_file = self.create_file_drop(
                self._node_id,
                os.path.join(self._volume, fits_file_name),
                oid='fits_file',
            )
            # The order of arguments is important so don't put anything in front of these
            copy_from_s3.addInput(s3_drop)
            copy_from_s3.addOutput(fits_file)

            if parallel_streams[counter] is not None:
                copy_from_s3.addInput(parallel_streams[counter])

            # Do the conversions
            convert_jpeg2000 = self.create_docker_app(
                self._node_id,
                get_module_name(DockerApp),
                'app_convert_jpeg2000',
                CONTAINER_SV,
                'sv_convert --input %i0 --input-options normalisation-dist LOG normalisation-domain CHANNEL --output %o0 --output-options Clayers=15 Clevels=6 Cycc=no Corder=CPRL ORGgen_plt=yes '
                'Cprecincts="{128,128},{64,64}" Cblk="{32,32}" Qstep=0.0001 --stats',
                user='root',
            )

            jpeg2000_name = self._get_jpeg2000_name(s3_object)
            jpeg2000_file = self.create_file_drop(
                self._node_id,
                os.path.join(self._volume, jpeg2000_name),
                oid='jpeg200_file',
            )

            convert_jpeg2000.addInput(fits_file)
            convert_jpeg2000.addOutput(jpeg2000_file)

            copy_jpg2000_to_s3 = self.create_app(
                self._node_id,
                get_module_name(CopyJpeg2000ToS3),
                'app_copy_jpeg_to_s3',
            )
            s3_jpeg2000_drop_out = self.create_s3_drop(
                self._node_id,
                self._bucket_name,
                '{0}/image_{1}_{2}.jpx'.format(
                    self._s3_jpeg2000_name,
                    minimum_frequency,
                    maximum_frequency,
                ),
                'aws-chiles02',
                's3_out',
            )
            copy_jpg2000_to_s3.addInput(jpeg2000_file)
            copy_jpg2000_to_s3.addOutput(s3_jpeg2000_drop_out)

            parallel_streams[counter] = s3_jpeg2000_drop_out

            counter += 1
            if counter >= self._parallel_streams:
                counter = 0

        barrier_drop = self.create_app(
            self._node_id,
            get_module_name(BarrierAppDROP),
            'app_barrier',
        )

        for jpeg2000_file in parallel_streams:
            if jpeg2000_file is not None:
                barrier_drop.addInput(jpeg2000_file)
        carry_over_data = self._map_carry_over_data[self._node_id]
        carry_over_data.barrier_drop = barrier_drop

        self.copy_logfiles_and_shutdown(True)

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
