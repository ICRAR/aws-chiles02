"""

"""
from sqlalchemy import MetaData, Table, Column, String, BigInteger, Integer, Index, Float, ForeignKey, TIMESTAMP

MAGPHYS_METADATA = MetaData()

DAY_NAME = Table(
    'day_name',
    MAGPHYS_METADATA,
    Column('day_name_id', BigInteger, primary_key=True),
    Column('name', String(400), nullable=False),
    Column('update_time', TIMESTAMP, nullable=False),

    Index('index_main', 'name'),
)

VISSTAT = Table(
    'visstat',
    MAGPHYS_METADATA,
    Column('visstat_id', BigInteger, primary_key=True),
    Column('visstat_meta_id', BigInteger, ForeignKey('visstat_meta.visstat_meta_id'), nullable=False),
    Column('sequence', Integer, nullable=False),
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

    Index('index_main', 'sequence'),
)

VISSTAT_META = Table(
    'visstat_meta',
    MAGPHYS_METADATA,
    Column('visstat_meta_id', BigInteger, primary_key=True),
    Column('day_name_id', BigInteger, ForeignKey('day_name.day_name_id'), nullable=False),
    Column('min_frequency', Integer, nullable=False),
    Column('max_frequency', Integer, nullable=False),

    Column('number_SI', Integer, nullable=False),
    Column('number_spectral_windows', Integer, nullable=False),
    Column('number_channels', Integer, nullable=False),
    Column('update_time', TIMESTAMP, nullable=False),

    Index('index_main', 'day_name_id', 'min_frequency', 'max_frequency'),
)
