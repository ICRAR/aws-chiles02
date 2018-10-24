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
Set up the system
"""
from setuptools import setup

setup(
    name='aws_chiles02',
    version='0.1',
    description='The CHILES pipeline for producing images using AWS',
    author='',
    author_email='',
    url='',

    # Keep alpha-sorted PLEASE!
    install_requires=[
        'argparse',
        'boto3',
        'ConfigObj',
        'dlg',
        'jsonpickle',
        'mako',
        'pip',
        'requests',
        'six',
    ],
    packages=[
        'aws_chiles02',
        'casa_code',
        'user_data',
    ],
    package_data={
        'user_data': ['*.bash', '*.yaml'],
    },
    entry_points={
        'console_scripts': [
            'generate=aws_chiles02.generate:main',
            'check_splits=aws_chiles02.check_splits:main',
            'check_clean=aws_chiles02.check_clean:main',
            'check_uvsub=aws_chiles02.check_uvsub:main',
        ]
    },

    # I need the daliuge client from github
    dependency_links=[
        'git+https://github.com/ICRAR/daliuge.git'
    ],
)
