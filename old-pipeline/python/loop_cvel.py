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
Taken from makecube.py extracting the loop over cvel

This module should run together with the casapy: e.g. casapy --nologger -c loop_cvel.py
"""
import os

from makecube_defines import check_dir, get_my_obs, vis_bk_dirs, vis_dirs, obs_dir, do_cvel, freq_max, freq_min, freq_step, freq_width, spec_window, INPUT_VIS_SUFFIX


# loop through selected obs and cvel. Uses obId to only do subset of possible
check_dir(vis_dirs)
check_dir(vis_bk_dirs)

obs_list = get_my_obs(obs_dir)
# dump_all()

for obs in obs_list:
    infile_dir = os.path.join(obs_dir, obs)

    infile = None
    for ff in os.listdir(infile_dir):
        if ff.endswith(INPUT_VIS_SUFFIX):
            infile = '{0}/{1}'.format(infile_dir, ff)
    if not infile:
        print 'No measurementSet file found under {0}'.format(infile_dir)
        continue

    obsId = os.path.basename(infile_dir).replace('_FINAL_PRODUCTS', '')
    outdir = '{0}/{1}/'.format(vis_dirs, obsId)
    backup_dir = '{0}/{1}/'.format(vis_bk_dirs, obsId)

    # dump_all()
    do_cvel(infile, outdir, backup_dir, freq_min, freq_max, freq_step, freq_width, spec_window, obsId)
