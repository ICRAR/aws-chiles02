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

This module contains all the setups and the defines
15/02/15 --- Merge chiles and chiles_pipeline

"""
import os
import commands
import re
import shutil
import time
import os.path

from echo import echo
from freq_map import freq_map
from taskinit import *
from mstransform import mstransform
from clean import clean
from task_exportfits import exportfits


casalog.filter('DEBUGGING')
INPUT_VIS_SUFFIX = '_calibrated_deepfield.ms'


@echo
def execCmd(cmd, failonerror=True, okErr=[]):
    """
    Execute OS command from within Python
    """
    re = commands.getstatusoutput(cmd)
    if re[0] != 0 and not (re[0] in okErr):
        errmsg = 'Fail to execute command: "%s". Exception: %s' % (cmd, re[1])
        if failonerror:
            raise Exception(errmsg)
        else:
            print errmsg
    return re


@echo
def get_my_obs(obs_dir):
    return os.listdir(obs_dir)


@echo
def check_dir(this_dir, create_on_missing=True):
    """
    Return    True if the directory is there
    """
    if not os.path.exists(this_dir):
        if create_on_missing:
            cmd = 'mkdir -p %s' % this_dir
            execCmd(cmd)

            if not os.path.exists(this_dir):
                raise Exception('Fail to create directory %s' % this_dir)
            return True
        else:
            return False
    else:
        return True


@echo
def create_cube_done_marker(casa_workdir, run_id, freq_range):
    return '%s/%s_cube_%s_done' % (casa_workdir, run_id, freq_range.replace('~', '-'))


@echo
def do_cube(in_dirs, cube_dir, min_freq, max_freq, step_freq, width_freq):
    outfile = os.path.join(cube_dir, 'cube_{0}~{1}'.format(min_freq, max_freq))
    print '''
Job {0}: clean(vis={1}, imagename={2})'''.format(job_id, str(in_dirs), outfile)
    if not debug:
        try:
            # dump_all()
            clean(vis=in_dirs,
                  imagename=outfile,
                  field='deepfield',
                  spw='',
                  mode='frequency',
                  restfreq='1420.405752MHz',
                  nchan=-1,
                  start='',
                  width='',
                  interpolation='nearest',
                  niter=1000,
                  gain=0.1,
                  threshold='0.0mJy',
                  imsize=[2048],
                  cell=['1.25arcsec'],
                  weighting='natural',
                  usescratch=True)
        except Exception, clEx:
            print '*********\nClean exception: %s\n***********' % str(clEx)


@echo
def cube_to_fits(cube_dir, min_freq, max_freq):
    outfile = os.path.join(cube_dir, 'cube_{0}~{1}'.format(min_freq, max_freq))
    print '''
Job {0}: exportfits(imagename={1})'''.format(job_id, outfile)
    if not debug:
        try:
            # dump_all()
            exportfits(imagename=outfile,
                       fitsimage=outfile.replace('image','fits'),
                       velocity=False,
                       optical=False,
                       bitpix=-32,
                       minpix=0,
                       maxpix=0,
                       overwrite=True,
                       dropstokes=True,
                       stokeslast=True,
                       history=False,
                       dropdeg=True)
        except Exception, clEx:
            print '*********\nExportFits exception: %s\n***********' % str(clEx)


@echo
def combineAllCubes(cube_dir, outname, min_freq, max_freq, step_freq, casa_workdir, run_id, debug, timeout=100):
    if sel_freq:
        steps = (max_freq - min_freq) / step_freq
        rem = (max_freq - min_freq) % step_freq
        if (rem):
            steps += 1
    else:
        steps = 1

    cube_names = []

    missing_freqs = []
    done_freq = {}

    # first wait for all freq splits are "cleaned" into sub-cubes
    for j in range(timeout):
        freq1 = min_freq
        freq2 = min_freq + step_freq
        for i in range(steps):
            if sel_freq:
                if rem and (i == steps - 1):
                    freq_range = '%d~%d' % (min_freq + i * step_freq, max_freq)
                else:
                    freq_range = str(freq1) + '~' + str(freq2)
            else:
                freq_range = 'min~max'
            if not done_freq.has_key(freq_range):
                done_02_f = create_cube_done_marker(casa_workdir, run_id, freq_range)
                if os.path.exists(done_02_f):
                    done_freq[freq_range] = 1
            freq1 = freq1 + step_freq
            freq2 = freq2 + step_freq
        gap = steps - len(done_freq.keys())
        if 0 == gap:
            break
        else:
            if j % 60 == 0:  # report every one minute
                print 'Still need %d freq to be cubed' % gap
            time.sleep(1)

    gap = steps - len(done_freq.keys())
    if gap > 0:
        print 'job %d timed out when waiting for concatenation, steps = %d, but done_freq = %d' % (job_id, steps, len(done_freq.keys()))
    else:
        print 'job %d found all sub-cubes are ready' % job_id

    # then loop through to form the cube name list for final concatenation
    freq1 = min_freq
    freq2 = min_freq + step_freq
    for i in range(steps):
        if sel_freq:
            if rem and (i == steps - 1):
                freq_range = '%d~%d' % (min_freq + i * step_freq, max_freq)
            else:
                freq_range = str(freq1) + '~' + str(freq2)
        else:
            freq_range = 'min~max'

        in_files = []
        outfile = cube_dir + 'cube_' + freq_range

        freq1 = freq1 + step_freq
        freq2 = freq2 + step_freq

        subcube = outfile + '.image'
        if not debug:
            if os.path.isdir(subcube):
                # cube_names = np.append(cube_names,subcube)
                # cube_names = cube_names + [subcube]
                cube_names.append(subcube)
            else:
                print 'WARNING, subcube is missing : ' + subcube
        else:
            # cube_names = cube_names + [subcube]
            cube_names.append(subcube)

    if debug:
        print '\nJob %d: Concatenating all cubes...\n\tia.imageconcat(infiles=%s,outfile=%s,relax=T)' % (job_id, str(cube_names), outname)
    else:
        print 'Start concatenating %s' % str(cube_names)
        final = ia.imageconcat(infiles=cube_names, outfile=outname, relax=True)
        final.done()

    return


@echo
def createSplitDoneMarker(casa_workdir, run_id, obsId):
    return '%s/%s__%s__split_done' % (casa_workdir, run_id, obsId)


@echo
def checkIfAllObsSplitDone(casa_workdir, job_id, run_id, all_obs, timeout=100):
    """
    """
    print 'Waiting for all obs split to be completed....'
    done_obs = {}  #
    for i in range(timeout):
        for obs in all_obs:
            infile_dir = '%s/%s' % (obs_dir, obs)
            obsId = os.path.basename(infile_dir).replace('_FINAL_PRODUCTS', '')
            if done_obs.has_key(obsId):
                continue
            done_01_f = createSplitDoneMarker(casa_workdir, run_id, obsId)
            if os.path.exists(done_01_f):
                done_obs[obsId] = 1
        gap = len(all_obs) - len(done_obs.keys())
        if 0 == gap:
            break
        else:
            if i % 60 == 0:  # report every one minute
                print 'Still need %d obs to be split' % gap
            time.sleep(1)

    gap = len(all_obs) - len(done_obs.keys())
    if gap > 0:
        print 'job %d timed out when waiting for all obs to be split' % job_id
    else:
        print 'job %d found all obs have been split' % job_id
    return done_obs


# load all environment variables to set up configuration

@echo
def do_cvel(infile, outdir, backup_dir, min_freq, max_freq, step_freq, width_freq, spec_window, obsId):
    """
    Adapted from loop.split.py with changes
    (1) deal with (max_freq - min_freq) % step_freq > 0
    (2) some debug info
    (3) obsId parameter for marking split_done

    If spec_window is blank ('') then freq_map is called to define the spw selection
    """
    if not os.path.exists(outdir):
        os.system('mkdir ' + outdir)
    if not os.path.exists(backup_dir):
        os.system('mkdir ' + backup_dir)

    steps = (max_freq - min_freq) / step_freq
    rem = (max_freq - min_freq) % step_freq
    if rem:
        steps += 1
    freq1 = min_freq
    freq2 = min_freq + step_freq
    bottom_edge = re.search('_[0-9]{3}_',infile)
    if bottom_edge:
        bedge = bottom_edge.group(0)
        bedge = int(bedge[1:4])

    if not sel_freq:
        steps = 1

    for i in range(steps):
        if sel_freq:
            #if (rem and (i == steps - 1)):
            #    freq_range = '%d~%d' % (min_freq + i * step_freq, max_freq)
            #else:
            #    freq_range = str(freq1) + '~' + str(freq2)
            if rem and i == steps - 1:
                freq_range = '%d~%d' % (min_freq + i * step_freq, max_freq)
                cvel_freq_range = '%f~%f' % (min_freq - 2 + i * step_freq, max_freq + 2)
            else:
                freq_range = str(freq1) + '~' + str(freq2)
                cvel_freq_range =  str(int(freq1-2)) + '~' + str(int(freq2+2))
            spw_range = spec_window + ':' + freq_range + 'MHz'
            cvel_spw_range = spec_window + ':' + cvel_freq_range + 'MHz'
            # spanning spectral windows and selecting freq fails
            # so use freq_map
            # THEREFORE ~10 lines above are IGNORED!!
            cvel_spw_range = freq_map(freq1, freq2, bedge)
        else:
            freq_range = 'min~max'
            spw_range = spec_window

        # If no spw is given then calculate from the max and min range
        if spec_window == '':
            spw_range = freq_map(freq1, freq2)

        no_chan = int(step_freq * 1000.0 / width_freq)  # MHz/kHz!!
        # I think I always want this with cvel.

        outfile = outdir + 'vis_' + freq_range
        backupfile = backup_dir + 'vis_' + freq_range
        if not debug:
            if os.path.exists(outfile):
                shutil.rmtree(outfile)
            if os.path.exists(backupfile):
                shutil.rmtree(backupfile)
            print 'working on: ' + outfile
            try:
                # dump_all()
                mstransform(
                    vis=infile,
                    outputvis=outfile,
                    regridms=True,
                    restfreq='1420.405752MHz',
                    mode='frequency',
                    nchan=no_chan,
                    outframe='lsrk',
                    interpolation='linear',
                    veltype='radio',
                    start=str(freq1) + 'MHz',
                    width=str(width_freq) + 'kHz',
                    spw=cvel_spw_range,
                    combinespws=True,
                    nspw=1,
                    createmms=False,
                    datacolumn="data")

                # cvel(vis=infile,
                # outputvis=outfile,
                #       restfreq='1420.405752MHz',
                #       mode='frequency',
                #       nchan=no_chan,
                #       outframe='lsrk',
                #       interpolation='linear',
                #       veltype='radio',
                #       start=str(freq1)+'MHz',
                #       width=str(width_freq)+'kHz',
                #       spw=spw_range)
            except Exception, spEx:
                print '*********\nSplit exception: %s\n***********' % str(spEx)
        else:
            msg = '''
mstransform(vis=%s,
outputvis=%s,
start=%s,
width=%s,
spw=%s,
nchan=%d)'''.format(infile, outfile, str(freq1)+'MHz', width_freq, cvel_spw_range, no_chan)
            print msg

        freq1 = freq1 + step_freq
        freq2 = freq2 + step_freq

    return


debug = int(os.getenv('CH_MODE_DEBUG', '0'))
null_str = 'N/A'
null_int = '-1'
spec_window = os.getenv('CH_SPW', '*')
if 'ALL' == spec_window.upper():
    spec_window = '*'
host_name = os.getenv('HOSTNAME', 'myhost')
job_id = int(os.getenv('CH_JOB_ID', '-1'))
run_id = os.getenv('CH_RUN_ID', null_str)
obs_dir = os.getenv('CH_OBS_DIR', null_str)
obs_first = int(os.getenv('CH_OBS_FIRST', null_int))
obs_last = int(os.getenv('CH_OBS_LAST', null_int))
freq_min = int(os.getenv('CH_FREQ_MIN', null_int))
freq_max = int(os.getenv('CH_FREQ_MAX', null_int))
freq_width = float(os.getenv('CH_FREQ_WIDTH', null_int))
freq_step = int(os.getenv('CH_FREQ_STEP', null_int))
vis_dirs = os.getenv('CH_VIS_DIR', null_str)
vis_bk_dirs = os.getenv('CH_VIS_BK_DIR', null_str)
casa_workdir = os.getenv('CH_CASA_WORK_DIR', null_str)
num_jobs = int(os.getenv('CH_NUM_JOB', null_int))
target_field = (os.getenv('CH_TARGET_FIELD', null_str))

split_tmout = int(os.getenv('CH_SPLIT_TIMEOUT', null_int))
if -1 == split_tmout:
    split_tmout = 3600
clean_tmout = int(os.getenv('CH_CLEAN_TIMEOUT', null_int))
if -1 == clean_tmout:
    clean_tmout = 3600

bkp_split = int(os.getenv('CH_BKP_SPLIT', '0'))
sel_freq = int(os.getenv('CH_SLCT_FREQ', '1'))

cube_dir = os.getenv('CH_CUBE_DIR', null_str) + '/'
out_dir = os.getenv('CH_OUT_DIR', null_str) + '/'

outname = '%s/comb_%d~%d.image' % (out_dir, freq_min, freq_max)
