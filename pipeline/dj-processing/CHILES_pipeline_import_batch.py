# CHILES_pipeline_import_batch
#
# This script is designed to get the parameters needed to process a given dataset
# and then run the CHILES pipeline modules.  It will do this by taking a number 
# as an input and then reading the parameters from a file.  It will then read the
# input parameters needed to run the pipeline, check them, and run the 6 pipeline
# modules.  It is designed to be run from the parent directory containing the
# sub-directories with the SDM files.  It will navigate to the directory 
# containing the SDM file of interest and then return to the parent directory
# on completion.
#
# This script is designed to allow a batch run of the pipeline on the WVU cluster,
# but can be run on any system. This module is used instead of CHILES_pipeline.py
#
# This script should be run from the command line in the following way:
# casa --nogui --nologger --agg -c "var=[sessionnumber]; execfile('path/CHILES_pipeline_batch.py')"
#
# 8/29/18 DJP
# 01/20/19 DJP: Just imports a bunch of sessions using initial module


import os
import glob
from numpy import loadtxt

# Set pipeline path
#pipepath='/data/dpisano/CHILES/chiles_pipeline/'
#pipepath='/lustre/aoc/projects/chiles/chiles_pipeline/'
#pipepath='/users/djpisano/chiles_pipeline/'
pipepath='/users/djpisano/cluster_pipeline/'

#Determine which dataset to process

try:
    sessionnum=int(var[0])
except IndexError:
    print('***ERROR:  Must pick a session number to run on. ***')
    print('***ERROR:  Enter an integer for a given run. ***')
   
# Read in parameters for pipeline run from a file
fname=pipepath+'chiles_parameters.txt'
headlen=2

# Create list with relevant parameters
par=[]
#Import file name
n=loadtxt(fname,str,skiprows=headlen,delimiter=';',usecols=[2])
par.append(n[sessionnum])
#Import bad antenna list
ba=loadtxt(fname,str,skiprows=headlen,delimiter=';',usecols=[3])
par.append(ba[sessionnum])
#Import reference antenna list
ra=loadtxt(fname,str,skiprows=headlen,delimiter=';',usecols=[4])
par.append(ra[sessionnum])
#Import scan list
sl=loadtxt(fname,str,skiprows=headlen,delimiter=';',usecols=[5])
par.append(sl[sessionnum])

# Set parent data directory (so data can be found)
#datapath='/data/dpisano/CHILES/DATA/'
#datapath='/lustre/aoc/projects/chiles/phase2/'
datapath='/scratch/djpisano/'
if os.getcwd() != datapath:
    os.chdir(datapath)

#Call CHILES_pipeline script:
# In all cases, we will want to Hanning smooth the data (when using this module):
myHanning='y'

try:
# Check that all variables are set and assign values to variables
    SDM_name=par[0]
    badants=par[1]
    refAnt=par[2]
    scanlist=par[3]
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
    os.chdir(datapath)
except IndexError:
    print ('***ERROR:  File selected is missing.***')
    print ('***ERROR:  Or parameter not set properly.***')


print('***Pipeline Run completed***')
