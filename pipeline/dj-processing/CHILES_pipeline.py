# CHILES_pipeline.py
# This script is designed as a wrapper for the CHILES pipeline.  It will take
# the input parameters and check that they are okay before running the 6 pipeline
# modules.  It is designed to be run from the parent directory containing the
# sub-directories with the SDM files.  It will navigate to the directory 
# containing the SDM file of interest and then return to the parent directory
# on completion.
# This script should be run from the command line in the following way:
# casa --nogui --nologger --agg -c "var=[SDM_name,badants,refAnt,scanlist]; execfile('path/CHILES_pipeline.py')"
# 8/8/18 DJP

import os
import glob

# Set pipeline path
#pipepath='/data/dpisano/CHILES/chiles_pipeline/'
#pipepath='/lustre/aoc/projects/chiles/chiles_pipeline/'
pipepath='/users/djpisano/cluster_pipeline/'

# Set parent data directory (so data can be found)
#datapath='/data/dpisano/CHILES/DATA/'
#datapath='/lustre/aoc/projects/chiles/phase2/'
datapath='/scratch/djpisano/'
if os.getcwd() != datapath:
    os.chdir(datapath)

# In all cases, we will want to Hanning smooth the data (when using this module):
myHanning='y'

try:
# Check that all variables are set and assign values to variables
    SDM_name=var[0]
    badants=var[1]
    refAnt=var[2]
    scanlist=var[3]
# If variables set correctly, then switch to dir containing SDM_name
    try:
        dirs=glob.glob(datapath+'/*/'+SDM_name)
        if len(dirs)>1:
            raise ValueError
        sessiondir=dirs[0].rstrip(SDM_name)
        os.chdir(sessiondir)
    except ValueError:
        print('***ERROR: More than one sub-directory contains the SDM file.***')
        print('***ERROR: Only one copy of SDM file should be in data directories.***')
# If set, then execute script
    execfile(pipepath+'CHILES_pipe_initial.py')
    execfile(pipepath+'CHILES_pipe_bandpass.py')
    execfile(pipepath+'CHILES_pipe_phasecal.py')
    execfile(pipepath+'CHILES_pipe_target.py')
    execfile(pipepath+'CHILES_pipe_split.py')
#    execfile(pipepath+'CHILES_pipe_QA.py')
    os.chdir(datapath)
except IndexError:
    print ('***ERROR:  Need to set variables before starting script.***')
    print ('***ERROR:  In order these are: SDM_name, myHanning, badants, refAnt, & scanlist.***')
    print ('***ERROR:  If there is no value for badants or scanlist, just enter empty string.***')


print('***Pipeline Run completed***')
