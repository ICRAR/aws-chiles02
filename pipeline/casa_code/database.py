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
Database tables for the stats
"""
from sqlalchemy import MetaData, Table, Column, String, BigInteger, Integer, Index, Float, ForeignKey, TIMESTAMP

CHILES02_METADATA = MetaData()

DAY_NAME = Table(
    'day_name',
    CHILES02_METADATA,
    Column('day_name_id', BigInteger, primary_key=True),
    Column('name', String(400), nullable=False),
    Column('update_time', TIMESTAMP, nullable=False),

    Index('index1', 'name', unique=True),
)

VISSTAT = Table(
    'visstat',
    CHILES02_METADATA,
    Column('visstat_id', BigInteger, primary_key=True),
    Column('visstat_meta_id', BigInteger, ForeignKey('visstat_meta.visstat_meta_id'), nullable=False),
    Column('scan', Integer, nullable=False),
    Column('spectral_window', Integer, nullable=False),
    Column('channel', Integer, nullable=False),
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
    Column('update_time', TIMESTAMP, nullable=False),

    Index('index1', 'visstat_meta_id', 'scan', 'spectral_window', 'channel', unique=True),
)

VISSTAT_META = Table(
    'visstat_meta',
    CHILES02_METADATA,
    Column('visstat_meta_id', BigInteger, primary_key=True),
    Column('width', Integer, nullable=False),
    Column('day_name_id', BigInteger, ForeignKey('day_name.day_name_id'), nullable=False),
    Column('min_frequency', Integer, nullable=False),
    Column('max_frequency', Integer, nullable=False),
    Column('update_time', TIMESTAMP, nullable=False),

    Index('index1', 'width', 'day_name_id', 'min_frequency', 'max_frequency'),
)
