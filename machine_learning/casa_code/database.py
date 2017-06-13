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

from sqlalchemy import BigInteger, Column, Float, ForeignKey, Integer, MetaData, String, Table

SQLITE = 'sqlite:///'
DATABASE_PATH = '/mnt/data/scan_statistics.sqlite'
LOG = logging.getLogger(__name__)

METADATA = MetaData()

OBSERVATION = Table(
    'day',
    METADATA,
    Column('observation_id', BigInteger, primary_key=True),
    Column('description', String(128), nullable=False),
)

SCAN = Table(
    'scan',
    METADATA,
    Column('scan_id', BigInteger, primary_key=True),
    Column('observation_id', BigInteger, ForeignKey('layer.layer_id'), nullable=False),
    Column('scan_number', Integer, nullable=False),
    Column('begin_time', Float, nullable=False),
    Column('end_time', Float, nullable=False),
    Column('begin_hour_angle', Float, nullable=False),
    Column('end_hour_angle', Float, nullable=False),
    Column('spectral_window', Integer, nullable=False),
    Column('channel', Integer, nullable=False),
    Column('frequency', Float, nullable=False),
    Column('max', Float, nullable=False),
    Column('mean', Float, nullable=False),
    Column('medabsdevmed', Float, nullable=False),
    Column('median', Float, nullable=False),
    Column('min', Float, nullable=False),
    Column('npts', Float, nullable=False),
    Column('quartile', Float, nullable=False),
    Column('rms', Float, nullable=False),
    Column('stddev', Float, nullable=False),
    Column('sum', Float, nullable=False),
    Column('sumsq', Float, nullable=False),
    Column('var', Float, nullable=False),
    sqlite_autoincrement=True,
)
