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

LOG = logging.getLogger(__name__)


class ErrorHandling(object):
    def __init__(self, oid, uid, **kwargs):
        super(ErrorHandling, self).__init__()
        LOG.info('oid = {0}, uid= {1}, kwargs={2}'.format(oid, uid, kwargs))
        self._session_id = kwargs['session_id']
        self._oid = oid
        self._uid = uid


class MockBarrierApp(object):
    def __init__(self, oid, uid, **kwargs):
        super(MockBarrierApp, self).__init__(oid, uid, **kwargs)
        LOG.info('oid = {0}, uid= {1}, kwargs={2}'.format(oid, uid, kwargs))


class MyTestClass(MockBarrierApp, ErrorHandling):
    def __init__(self, *args, **kwargs):
        super(MyTestClass, self).__init__(*args, **kwargs)


if __name__ == "__main__":
    my_class = MyTestClass('oid', 'uid', session_id='session_id', dummy01='dummy01')
