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
Taken from makecube.py extracting the loop over clean

This module should run together with the casapy: e.g. casapy --nologger -c loop_clean.py
"""
import fnmatch
import os
from os.path import join, exists

from makecube_defines import check_dir, cube_dir, vis_dirs, run_id, do_cube, freq_min, freq_max, freq_step, freq_width


check_dir(cube_dir)

print '''
vis_dirs = {0}
run_id   = {1}'''.format(vis_dirs, run_id)

vis_dirs_cube = []
for root, dir_names, filenames in os.walk(vis_dirs):
    for match in fnmatch.filter(dir_names, 'vis_*'):
        full_dir_name = join(root, match)
        # Make sure it contains data
        if exists(join(full_dir_name, 'table.dat')):
            vis_dirs_cube.append('{0}'.format(full_dir_name))

do_cube(vis_dirs_cube, cube_dir, freq_min, freq_max, freq_step, freq_width)

# Done
