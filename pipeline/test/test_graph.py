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
Test the Chiles02 pipeline
"""
import logging
import os
import unittest
import uuid

from configobj import ConfigObj

from aws_chiles02.common import make_groups_of_frequencies
from dlg import droputils
from dlg.apps.dockerapp import DockerApp
from dlg.drop import BarrierAppDROP, DirectoryContainer, FileDROP
from mock.s3_drop import MockS3DROP

logging.basicConfig(level=logging.DEBUG)


class TestChiles02(unittest.TestCase):
    _temp = None

    @classmethod
    def setUpClass(cls):
        config_file_name = os.path.join(os.path.expanduser('~'), '.dlg/dlg.settings')
        if os.path.exists(config_file_name):
            config = ConfigObj(config_file_name)
            TestChiles02._temp = config.get('OS_X_TEMP')

            if not os.path.exists(TestChiles02._temp):
                os.makedirs(TestChiles02._temp)

        if TestChiles02._temp is None:
            TestChiles02._temp = '/tmp'

    def setUp(self):
        self._counters = {}

    def test_start_graph(self):
        s3_drop = MockS3DROP(self.get_oid('s3'), uuid.uuid4(), bucket='mock', key='key123', profile_name='aws-profile')
        copy_from_s3 = DockerApp(self.get_oid('app'), uuid.uuid4(), image='mock:latest', command='copy_from_s3.sh %iDataURL0 /dlg_root/%o0', user='root')
        measurement_set = DirectoryContainer(self.get_oid('dir'), uuid.uuid4(), dirname=TestChiles02._temp)
        copy_from_s3.addInput(s3_drop)
        copy_from_s3.addOutput(measurement_set)

        with droputils.DROPWaiterCtx(self, measurement_set, 50000):
            s3_drop.setCompleted()

    def test_to_first_split(self):
        s3_drop = MockS3DROP(self.get_oid('s3'), uuid.uuid4(), bucket='mock', key='key123', profile_name='aws-profile')
        copy_from_s3 = DockerApp(self.get_oid('app'), uuid.uuid4(), image='mock:latest', command='copy_from_s3.sh %iDataURL0 /dlg_root/%o0', user='root')
        measurement_set = DirectoryContainer(self.get_oid('dir'), uuid.uuid4(), dirname=TestChiles02._temp)
        copy_from_s3.addInput(s3_drop)
        copy_from_s3.addOutput(measurement_set)

        outputs = []
        frequencies = make_groups_of_frequencies(FREQUENCY_GROUPS, 5)
        frequencies = frequencies[0]
        for group in frequencies:
            casa_py_drop = DockerApp(self.get_oid('app'), uuid.uuid4(), image='mock:latest', command='casa_py.sh /dlg_root/%i0 /dlg_root/%o0 {0} {1}'.format(group[0], group[1]), user='root')
            result = FileDROP(self.get_oid('file'), uuid.uuid4(), dirname=TestChiles02._temp)
            copy_to_s3 = DockerApp(self.get_oid('app'), uuid.uuid4(), image='mock:latest', command='copy_to_s3.sh /dlg_root/%i0 %oDataURL0', user='root')
            s3_drop_out = MockS3DROP(self.get_oid('s3'), uuid.uuid4(), bucket='mock', key='{0}_{1}/key123'.format(group[0], group[1]), profile_name='aws-profile')

            casa_py_drop.addInput(measurement_set)
            casa_py_drop.addOutput(result)
            copy_to_s3.addInput(result)
            copy_to_s3.addOutput(s3_drop_out)
            outputs.append(s3_drop_out)

        barrier_drop = BarrierAppDROP(self.get_oid('barrier'), uuid.uuid4())
        barrier_drop.addInput(measurement_set)

        for output in outputs:
            barrier_drop.addInput(output)

        with droputils.DROPWaiterCtx(self, barrier_drop, 50000):
            s3_drop.setCompleted()

    def test_to_split_combine(self):
        s3_drop = MockS3DROP(self.get_oid('s3'), uuid.uuid4(), bucket='mock', key='key123', profile_name='aws-profile')
        copy_from_s3 = DockerApp(self.get_oid('app'), uuid.uuid4(), image='mock:latest', command='copy_from_s3.sh %iDataURL0 /dlg_root/%o0', user='root')
        measurement_set = DirectoryContainer(self.get_oid('dir'), uuid.uuid4(), dirname=TestChiles02._temp)
        copy_from_s3.addInput(s3_drop)
        copy_from_s3.addOutput(measurement_set)

        barrier_drop = None
        for group in make_groups_of_frequencies(FREQUENCY_GROUPS, 5):
            outputs = []
            for frequeny_pairs in group:
                casa_py_drop = DockerApp(
                        self.get_oid('app'), uuid.uuid4(), image='mock:latest', command='casa_py.sh /dlg_root/%i0 /dlg_root/%o0 {0} {1}'.format(frequeny_pairs[0], frequeny_pairs[1]), user='root')
                result = FileDROP(self.get_oid('file'), uuid.uuid4(), dirname=TestChiles02._temp)
                copy_to_s3 = DockerApp(self.get_oid('app'), uuid.uuid4(), image='mock:latest', command='copy_to_s3.sh /dlg_root/%i0 %oDataURL0', user='root')
                s3_drop_out = MockS3DROP(self.get_oid('s3'), uuid.uuid4(), bucket='mock', key='{0}_{1}/key123'.format(frequeny_pairs[0], frequeny_pairs[1]), profile_name='aws-profile')

                casa_py_drop.addInput(measurement_set)
                casa_py_drop.addOutput(result)
                copy_to_s3.addInput(result)
                copy_to_s3.addOutput(s3_drop_out)
                outputs.append(s3_drop_out)

            barrier_drop = BarrierAppDROP(self.get_oid('barrier'), uuid.uuid4())
            barrier_drop.addInput(measurement_set)

            for output in outputs:
                barrier_drop.addInput(output)

        with droputils.DROPWaiterCtx(self, barrier_drop, 50000):
            s3_drop.setCompleted()

    def test_to_split_chain_combine(self):
        s3_drop = MockS3DROP(self.get_oid('s3'), uuid.uuid4(), bucket='mock', key='key123', profile_name='aws-profile')
        copy_from_s3 = DockerApp(self.get_oid('app'), uuid.uuid4(), image='mock:latest', command='copy_from_s3.sh %iDataURL0 /dlg_root/%o0', user='root')
        measurement_set = DirectoryContainer(self.get_oid('dir'), uuid.uuid4(), dirname=TestChiles02._temp)
        copy_from_s3.addInput(s3_drop)
        copy_from_s3.addOutput(measurement_set)

        outputs = []
        for group in make_groups_of_frequencies(FREQUENCY_GROUPS, 8):
            first = True
            end_of_last_element = None
            for frequeny_pairs in group:
                casa_py_drop = DockerApp(
                        self.get_oid('app'), uuid.uuid4(), image='mock:latest', command='casa_py.sh /dlg_root/%i0 /dlg_root/%o0 {0} {1}'.format(frequeny_pairs[0], frequeny_pairs[1]), user='root')
                result = FileDROP(self.get_oid('file'), uuid.uuid4(), dirname=TestChiles02._temp)
                copy_to_s3 = DockerApp(self.get_oid('app'), uuid.uuid4(), image='mock:latest', command='copy_to_s3.sh /dlg_root/%i0 %oDataURL0', user='root')
                s3_drop_out = MockS3DROP(self.get_oid('s3'), uuid.uuid4(), bucket='mock', key='{0}_{1}/key123'.format(frequeny_pairs[0], frequeny_pairs[1]), profile_name='aws-profile')

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

        barrier_drop = BarrierAppDROP(self.get_oid('barrier'), uuid.uuid4())
        barrier_drop.addInput(measurement_set)

        for output in outputs:
            barrier_drop.addInput(output)

        with droputils.DROPWaiterCtx(self, barrier_drop, 50000):
            s3_drop.setCompleted()

    def get_oid(self, count_type):
        count = self._counters.get(count_type)
        if count is None:
            count = 1
        else:
            count += 1
        self._counters[count_type] = count

        return '{0}__{1:06d}'.format(count_type, count)

if __name__ == '__main__':
    unittest.main()
