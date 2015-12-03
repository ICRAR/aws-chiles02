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

"""
import os
from os.path import basename

from taskinit import *
from cleanhelper import *


MNT_OUTPUT_CHILES = '/mnt/output/Chiles/'
MNT_INPUT_CHILES = '/mnt/input/Chiles/'


def fixed_frequency(cube_name):
    base_name = basename(cube_name)
    elements = base_name.split('.')
    elements = elements[0].split('_')
    elements = elements[1].split('~')
    return int(elements[0])


cube_names = []
out_name = MNT_OUTPUT_CHILES + os.getenv('IMAGE_NAME', 'image') + '.cube'
for dir_name in sorted(os.listdir(MNT_INPUT_CHILES)):
    if dir_name.endswith('.image'):
        path_join = os.path.join(MNT_INPUT_CHILES, dir_name)
        print 'Adding: {0}'.format(path_join)
        cube_names.append(path_join)

print 'List before sorting %s' % str(cube_names)

# Re-sort the cubes as the order is important
cube_names = sorted(cube_names, key=fixed_frequency)

print 'Start concatenating %s' % str(cube_names)
final = ia.imageconcat(infiles=cube_names, outfile=out_name, relax=True)
final.done()
