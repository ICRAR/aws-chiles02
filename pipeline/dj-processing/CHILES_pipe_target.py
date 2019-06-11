# -*- coding: utf-8 -*-
# CHILES_pipe_target.py
# This task runs after the "phasecal" task so all calibrations have been applied
# to the target field.  This task runs RFLAG and extend on the target field and
# makes some diagnostic plots.  
# 9/9/15 DJP
# Removed clipping due to overflagging 12/7/15 DJP
# Set extendpols=False in RFLAG 1/27/16 DJP
# Included flag summary for target, and applycal for target moved here, 2/9/16 DJP
# Removed averaging from plots, 2/11/16 DJP
# 2/15/16 DJP: Changed ff value to a float and re-implemented "extend".
# 2/18/16 DJP: Plot UV spectrum of field (averaging over time, baseline)
# 2/18/16 DJP: Make diagnostic plots using averaged data.
# 2/19/16 DJP: Make 2 UVSPEC plots (one with full range, one with zoom).  Changed averaging.  
# 6/28/16 DJP: Including time/frequency averaging on the target after initial rflag and extend.
# 7/1/16 DJP:  Including time-averaged RFLAG and using parameters determined by XF.  Setting rflag scale=3, growtime=60.  
# 9/21/16 DJP:  Fixed variable names for time-averaged RFLAG. 
# 12/7/16 DJP:  Changed plots to frequency on x-axis. Including amp v. time plots. Saving flagging statistics to file.
# 12/9/16 DJP:  Added additional flagging to completely flag any channel that is already more than 90% flagged (code from XF).
# 4/22/18 DJP: Changing flagging and split to oldsplit
# 8/29/18 DJP: Changed field='1' to field='deepfield' and other fields to their name instead of number.  



logprint ("Starting CHILES_pipe_target.py", logfileout='logs/target.log')
time_list=runtiming('target', 'start')

# If running this module right after starting casa, must run CHILES_pipe_restore first.
# Step 1, load needed functions.
import copy
import numpy as np
import pylab as pylab
import re as re
import sys

# Clean up data from past runs of module:
os.system("rm -rf images/target_spw*.*")
os.system("rm -rf plots/target_*.png")
os.system("rm -rf *_target_flux_averaged.ms")

logprint ("Apply calibration to target", logfileout='logs/target.log')

TargetTables=copy.copy(priorcals)
TargetTables.append('finalphase_scan.gcal')
TargetTables.append('finalamp.gcal')
TargetTables.append('finalflux.gcal')
TargetSpwMapValues=copy.copy(priorspwmap)
TargetSpwMapValues.append([])
TargetSpwMapValues.append([])
TargetSpwMapValues.append([])

if os.path.exists('antposcal.p')==True:
  TargetFields=['','','1331+305=3C286','1331+305=3C286','J0943-0819','J0943-0819','J0943-0819']

if os.path.exists('antposcal.p')==False:
  TargetFields=['','1331+305=3C286','1331+305=3C286','J0943-0819','J0943-0819','J0943-0819']

default('applycal')
vis=ms_active
field='deepfield'            # Apply calibration to target
spw=''
intent=''
selectdata=True
gaintable=TargetTables    # HG : Change this from AllCalTables to TargetTables
gainfield=TargetFields
interp=['']
spwmap=TargetSpwMapValues # Was [] in previous version, now corresponds to TargetTables
gaincurve=False
opacity=[]
parang=False
calwt=False
flagbackup=False
async=False
applycal()


# Step 2: Flagging of deepfield with Clipping & RFLAG & Time-averaged RFLAG
logprint("Flagging deepfield",logfileout='logs/target.log')
# New Flagging routines by XF
#Initial clip on spw 14 to exclude any high points
default('flagdata')
vis=ms_active
datacolumn='corrected'
field='deepfield'
spw='14'
mode='clip'
clipminmax=[0,30]
action='apply'
flagbackup=False
flagdata()

#This calculates timedev and freqdev for spw14
default('flagdata')
vis=ms_active
datacolumn='corrected'
field='deepfield'
spw='14'
scan=''
mode='rflag'
timedev='tdev14.txt'
freqdev='fdev14.txt'
freqdevscale=1.0
timedevscale=1.0
extendflags=False
action='calculate'
flagdata()


with open("tdev14.txt","r") as data:
    dictTime=ast.literal_eval(data.read())

with open("fdev14.txt","r") as data:
    dictFreq=ast.literal_eval(data.read())

timenoavg=dictTime["timedev"][0][2]
freqnoavg=dictFreq["freqdev"][0][2]

ff=float(1)
scaling1=[2.4,2.1,1.9,1.3,1.2,1.2,1.1,1.1,1.1,1.1,1.0,1.0,1.0,1.0,1.0]
scaling=np.asarray(scaling1)
sigmacut=8.0
freqd1noavg=scaling*freqnoavg*sigmacut
timed1noavg=scaling*timenoavg*sigmacut

default('flagdata')
vis=ms_active
mode='rflag'
field='deepfield'
spw='0~14'
correlation=''
ntime='scan'
combinescans=False
datacolumn='corrected'
extendflags=False       
extendpols=False      
winsize=3
freqdev=[[ff,0.0,freqd1noavg[0]],[ff,1.0,freqd1noavg[1]],[ff,2.0,freqd1noavg[2]],[ff,3.0,freqd1noavg[3]],[ff,4.0,freqd1noavg[4]],[ff,5.0,freqd1noavg[5]],[ff,6.0,freqd1noavg[6]],[ff,7.0,freqd1noavg[7]],[ff,8.0,freqd1noavg[8]],[ff,9.0,freqd1noavg[9]],[ff,10.0,freqd1noavg[10]],[ff,11.0,freqd1noavg[11]],[ff,12.0,freqd1noavg[12]],[ff,13.0,freqd1noavg[13]],[ff,14.0,freqd1noavg[14]]]
timedev=[[ff,0.0,timed1noavg[0]],[ff,1.0,timed1noavg[1]],[ff,2.0,timed1noavg[2]],[ff,3.0,timed1noavg[3]],[ff,4.0,timed1noavg[4]],[ff,5.0,timed1noavg[5]],[ff,6.0,timed1noavg[6]],[ff,7.0,timed1noavg[7]],[ff,8.0,timed1noavg[8]],[ff,9.0,timed1noavg[9]],[ff,10.0,timed1noavg[10]],[ff,11.0,timed1noavg[11]],[ff,12.0,timed1noavg[12]],[ff,13.0,timed1noavg[13]],[ff,14.0,timed1noavg[14]]]
timedevscale=1.0
freqdevscale=1.0
action='apply'
display=''
flagbackup=False
savepars=True
flagdata()

#This calculates timedev and freqdev for time-averaged data
# Removing time-averaged flagging for v4.2 of pipeline (we don't think CASA 5.3 actually works properly here)
# default('flagdata')
# vis=ms_active
# datacolumn='corrected'
# field='deepfield'
# spw='14'
# scan=''
# mode='rflag'
# timedev='tdev14_tavg.txt'
# freqdev='fdev14_tavg.txt'
# freqdevscale=1.0
# timedevscale=1.0
# timeavg=True
# timebin='1000s'
# extendflags=False
# action='calculate'
# flagdata()
# 
# 
# with open("tdev14_tavg.txt","r") as data:
#     dictTime=ast.literal_eval(data.read())
# 
# with open("fdev14_tavg.txt","r") as data:
#     dictFreq=ast.literal_eval(data.read())
# 
# timeavgval=dictTime["timedev"][0][2]
# freqavgval=dictFreq["freqdev"][0][2]
# 
# 
# sigmacut=8.0
# freqd1avg=scaling*freqavgval*sigmacut
# timed1avg=scaling*timeavgval*sigmacut
# 
# default('flagdata')
# vis=ms_active
# mode='rflag'
# field='deepfield'
# correlation=''
# ntime='scan'
# combinescans=False
# datacolumn='corrected'
# extendpols=False      
# extendflags=False        
# freqdev=[[ff,0.0,freqd1avg[0]],[ff,1.0,freqd1avg[1]],[ff,2.0,freqd1avg[2]],[ff,3.0,freqd1avg[3]],[ff,4.0,freqd1avg[4]],[ff,5.0,freqd1avg[5]],[ff,6.0,freqd1avg[6]],[ff,7.0,freqd1avg[7]],[ff,8.0,freqd1avg[8]],[ff,9.0,freqd1avg[9]],[ff,10.0,freqd1avg[10]],[ff,11.0,freqd1avg[11]],[ff,12.0,freqd1avg[12]],[ff,13.0,freqd1avg[13]],[ff,14.0,freqd1avg[14]]]
# timedev=[[ff,0.0,timed1avg[0]],[ff,1.0,timed1avg[1]],[ff,2.0,timed1avg[2]],[ff,3.0,timed1avg[3]],[ff,4.0,timed1avg[4]],[ff,5.0,timed1avg[5]],[ff,6.0,timed1avg[6]],[ff,7.0,timed1avg[7]],[ff,8.0,timed1avg[8]],[ff,9.0,timed1avg[9]],[ff,10.0,timed1avg[10]],[ff,11.0,timed1avg[11]],[ff,12.0,timed1avg[12]],[ff,13.0,timed1avg[13]],[ff,14.0,timed1avg[14]]]
# timedevscale=1.0
# freqdevscale=1.0
# channelavg=False
# chanbin=1
# timeavg=True
# timebin='1000s'
# action='apply'
# flagbackup=False
# savepars=True
# flagdata()


clearstat()

#EM: back to normal logger output

#casalog.filter("INFO")

# Summary of flagging, after final flagging (for testing purposes only)
logprint ("Summary of flags after flagging target", logfileout='logs/target.log')
default('flagdata')
vis=ms_active
mode='summary'
spw='0~14'
field='deepfield'           # Only flagged deepfield, no need to check others here.
correlation='RR,LL'
spwchan=True
spwcorr=True
basecnt = True
action='calculate'
s_t=flagdata()


target_flag=s_t['flagged']/s_t['total']
logprint ("Percentage of all data flagged after RFLAG+Extend+time-avg RFLAG: "+str(target_flag*100)+"%", logfileout='logs/target.log')

# Flagging any channel that is already more than 90% flagged.  
limFlag=0.9
flagChannels=[]
for a in s_t['spw:channel']:
    fp=s_t['spw:channel'][a]['flagged']/s_t['spw:channel'][a]['total']
    if  fp>limFlag:
        flagChannels.append(a)
strChan = ','.join(flagChannels)

flagdata(vis=ms_active,field='deepfield',mode="manual",spw=strChan, flagbackup=False, autocorr=False)


# Save final version of flags
logprint("Saving flags",logfileout='logs/target.log')
a
default('flagmanager')
vis=ms_active
mode='save'
versionname='targetflags'
comment='Final flags saved after calibrations and target rflag+extend'
merge='replace'
flagmanager()
logprint ("Flag column saved to "+versionname, logfileout='logs/target.log')



logprint ("Finished CHILES_pipe_target.py", logfileout='logs/target.log')
time_list=runtiming('target', 'end')

pipeline_save()
