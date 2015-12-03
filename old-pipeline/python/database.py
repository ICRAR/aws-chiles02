#
#    (c) UWA, The University of Western Australia
#    M468/35 Stirling Hwy
#    Perth WA 6009
#    Australia
#
#    Copyright by UWA, 2012-2015
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
The database tables
"""
from sqlalchemy import MetaData, Table, Column, Float, Integer, String

TRACE_METADATA = MetaData()

LOG_DETAILS = Table(
    'log_details',
    TRACE_METADATA,
    Column('log_id', Integer, primary_key=True),
    Column('pid', Integer, index=True),
    Column('timestamp', String(40), index=True),
    Column('total_cpu', Float),
    Column('kernel_cpu', Float),
    Column('vm', Float),
    Column('rss', Float),
    Column('iops', Float),
    Column('bytes_sec', Float),
    Column('io_wait', Float),
    sqlite_autoincrement=True
)

PROCESS_DETAILS = Table(
    'process_details',
    TRACE_METADATA,
    Column('process_id', Integer, primary_key=True),
    Column('pid', Integer, index=True),
    Column('ppid', Integer, index=True),
    Column('name', String(256)),
    Column('cmd_line', String(2000)),
    Column('create_time', Float),
    sqlite_autoincrement=True
)
