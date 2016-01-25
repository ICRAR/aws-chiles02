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
import unittest

from aws_chiles02.common import get_observation

logging.basicConfig(level=logging.DEBUG)


class TestCommon(unittest.TestCase):
    def test_get_observation(self):
        observation = get_observation('13B-266.sb28624226.eb28625769.56669.43262586805_calibrated_deepfield.ms')
        self.assertEquals('13B-266.sb28624226.eb28625769.56669.43262586805', observation)

if __name__ == '__main__':
    unittest.main()
