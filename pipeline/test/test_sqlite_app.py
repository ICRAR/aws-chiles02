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

"""
import logging
import os
import unittest

from configobj import ConfigObj

from aws_chiles02.apps import InitializeSqliteApp
from aws_chiles02.common import get_oid, get_uuid
from dfms import droputils
from dfms.drop import FileDROP, InMemoryDROP

logging.basicConfig(level=logging.DEBUG)


class TestSqlite(unittest.TestCase):
    _temp = None

    @classmethod
    def setUpClass(cls):
        config_file_name = os.path.join(os.path.expanduser('~'), '.dfms/dfms.settings')
        if os.path.exists(config_file_name):
            config = ConfigObj(config_file_name)
            TestSqlite._temp = config.get('OS_X_TEMP')

            if not os.path.exists(TestSqlite._temp):
                os.makedirs(TestSqlite._temp)

        if TestSqlite._temp is None:
            TestSqlite._temp = '/tmp'

    def test_sql_create(self):
        sqlite01 = get_oid('sqlite')
        sqlite_drop = FileDROP(
            sqlite01,
            get_uuid(),
            precious=False,
            dirname=os.path.join(TestSqlite._temp, sqlite01),
            check_exists=False,
        )
        initialize_sqlite = InitializeSqliteApp(
            get_oid('app'),
            get_uuid(),
            user='root',
        )
        sqlite_in_memory = InMemoryDROP(
            get_oid('memory'),
            get_uuid(),
            precious=False,
        )
        initialize_sqlite.addInput(sqlite_drop)
        initialize_sqlite.addOutput(sqlite_in_memory)

        with droputils.DROPWaiterCtx(self, sqlite_in_memory, 50000):
            sqlite_drop.setCompleted()

if __name__ == '__main__':
    unittest.main()
