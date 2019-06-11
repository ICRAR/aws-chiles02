# CHILES_pipeline_makepdf
#
# This script is designed to get the parameters needed to process a given dataset
# and then convert the html files to pdf files.  It will do this by taking a number 
# as an input and then reading the parameters from a file.  It will then read the
# input parameters needed to run the pipeline, check them, and run the 6 pipeline
# modules.  It is designed to be run from the parent directory containing the
# sub-directories with the SDM files.  It will navigate to the directory 
# containing the SDM file of interest and then return to the parent directory
# on completion.
#
# This script is designed to allow a batch run of the pipeline on the WVU cluster.
# 01/20/19 DJP


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

try:
# Check that all variables are set and assign values to variables
    SDM_name=par[0]
# If variables set correctly, then switch to dir containing SDM_name
    try:
        dirs=glob.glob(datapath+'/*/'+SDM_name)
        if len(dirs)>1:
            raise ValueError
        sessiondir=dirs[0].rstrip(SDM_name)
        os.chdir(sessiondir/FINAL)
    except ValueError:
        print('***ERROR: More than one sub-directory contains the SDM file.***')
        print('***ERROR: Only one copy of SDM file should be in data directories.***')
# If set, then execute script
    singularity exec wkhtmltox.simg wkhtmltopdf initial.html initial.pdf
    singularity exec wkhtmltox.simg wkhtmltopdf bandpass.html bandpass.pdf
    singularity exec wkhtmltox.simg wkhtmltopdf phasecal.html phasecal.pdf
    singularity exec wkhtmltox.simg wkhtmltopdf target.html target.pdf
    singularity exec wkhtmltox.simg wkhtmltopdf split.html split.pdf
    singularity exec wkhtmltox.simg wkhtmltopdf QA.html QA.pdf
    os.chdir(datapath)
except IndexError:
    print ('***ERROR:  File selected is missing.***')
    print ('***ERROR:  Or parameter not set properly.***')


print('***Pipeline Run completed***')
