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
Taken from makecube.py extracting the final combination of cubes

This module should run together with the casapy: e.g. casapy --logfile casapy.log -c make_finalcube.py
"""

execfile('/home/ec2-user/chiles_pipeline/python/makecube_defines.py')

check_dir(out_dir)


obs_list = get_my_obs(obs_dir)
obsId_list = []

print "myobs = \t%s\nvis_dirs = \t%s\nrun_id = \t%s" % (str(obs_list), vis_dirs, run_id)

# Wait on clean ...

if job_id == 0: # only the first job will do the final concatenation
    combineAllCubes(cube_dir,outname,freq_min,freq_max,freq_step,casa_workdir,
                    run_id, debug, timeout = clean_tmout)

