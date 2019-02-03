# CHILES_pipe_bandpass.py
# This module of the CHILES pipeline does the delay and bandpass correction
# on the data as well as the flux calibration.  It is heavily based on the code 
# written by Ximena Fernandez, and not on the NRAO continuum pipeline. 
# 8/27/15 DJP

#Need to run CHILES_pipe_restore.py first, if not immediately following CHILES_pipeline_initial.py
#8/31/15 DJP

# HG made a number of changes to make sure that the caltable lists and spw selection
# were done correctly.  I explicitly removed all prior caltables.  12/7/15 DJP

# 1/14/16, DJP: Set fillgaps=10 with interp='linear,spline' in bandpass (probably no effect)
#               Set interp=',spline' for BPcal tables for frequency interpolation.

# 1/27/16 DJP: Set extendpols=False in RFLAG step.
# 2/09/16 DJP: Remove interpolation settings from 1/14/16 and a bunch of the flag summaries
# 2/11/16 DJP: Solve for all channels.  Make cloud plots with no averaging
# 2/18/16 DJP: Make diagnostic plots with averaged, split data
# 2/18/16 DJP: Make plot of amp v. frequency for 3C286 averaged over time & baseline from UV data.
# 2/19/16 DJP: Make 2 UVSPEC plots (one with full range, one with zoom).  Changed averaging.
# 4/8/16 DJP: Set minsnr for calibration solution to 8.
# 6/9/16 DJP: Forgot to copy snrval to minsnr parameters, fixed.  Need to add option for user to specify snrval.
# 6/16/16 DJP: Removing images of flux calibrator from diagnostic plots.
# 6/22/16 DJP: including amp/phase vs. time plots and leaving minsnr at default values.
# 9/21/16 DJP:  Changed channel range for tst_bpass_spw (only excluding 50 edge channels).  Moved flagmanager to end.
# 10/28/16 DJP: Added backup of finalBPcal.b, (1/21/17 DJP: fixed bug)
# 1/10/17 DJP: Delete individual png bandpass plots as information is already in bandpass.pdf
# 5/13/17 DJP: Removed plotbandpass and replace with using plotms for refAnt.
# 4/22/18 DJP: Changing flagging and split to oldsplit
# 8/29/18 DJP: Changed field='2' to field='1331+305=3C286'.  
# 8/29/18 DJP: Including masks to avoid bad RFI while calibrating
# 10/8/18 DJP: Including flagging percentage vs. uvdist, using flags instead of masks.
# 11/6/18 DJP: Removing all plotting at end of module.  
# 12/19/18 DJP: Moved mask flags to initial module

#Part I: define some variables that will be used later
import copy
import numpy as np
import pylab as pylab
import re as re
import sys

logprint ("Starting CHILES_pipe_bandpass.py", logfileout='logs/bandpass.log')
time_list=runtiming('bandpass', 'start')

#Clean up calibration tables from past runs:
if os.path.exists('initialBPcal.b'):
    rmtables(tablenames='initialBPcal.b')
if os.path.exists('finalBPcal.b'):
    rmtables(tablenames='finalBPcal.b')
if os.path.exists('initialdelay.k'):
    rmtables(tablenames='initialdelay.k')
if os.path.exists('finaldelay.k'):
    rmtables(tablenames='finaldelay.k')
if os.path.exists('finalBPinitialgain.g'):
    rmtables(tablenames='finalBPinitialgain.g')
if os.path.exists('finaldelayinitialgain.g'):
    rmtables(tablenames='finaldelayinitialgain.g')
if os.path.exists('gain_curves.g'):
    rmtables(tablenames='gain_curves.g')
if os.path.exists('initialBPinitialgain.g'):
    rmtables(tablenames='initialBPinitialgain.g')
if os.path.exists('testdelayinitialgain.g'):
    rmtables(tablenames='testdelayinitialgain.g')
# Remove old images of flux calibrator
os.system("rm -rf images/fluxcalibrator_spw*.*")


#This creates an array that defines a channel range for each spectral window
tb.open(ms_active+'/SPECTRAL_WINDOW')
channels = tb.getcol('NUM_CHAN')
numSpws = len(channels)
tst_delay_spw=''
all_spw=''

for ispw in range(numSpws):
    endch1=int(channels[ispw]/3.0)
    endch2=int(2.0*channels[ispw]/3.0)+1
    if (ispw<max(range(numSpws))):
        tst_delay_spw=tst_delay_spw+str(ispw)+':'+str(endch1)+'~'+str(endch2)+','
        all_spw=all_spw+str(ispw)+','
    else:
        tst_delay_spw=tst_delay_spw+str(ispw)+':'+str(endch1)+'~'+str(endch2)
        all_spw=all_spw+str(ispw)

# Avoid solving for baseline on edge channels
tst_bpass_spw='0~14:50~1997'



# The following parameters are set in the initial pipeline script by default or user input.
##Set minimum # of baselines need for a solution to 8, based on experience
minBL_for_cal=8
#
##Set uvrange to apply in order to optimally exclude RFI without flagging:
uvr_cal='>1500m'

#Reference Antenna should have already been selected:
while bool(refAnt)==False:
    refAnt=raw_input("Enter name of one or more reference antenna(s) (e.g. ea01, ea02, etc.;this is needed to proceed): ")


#--------------------------------------------------------------------
# Part II:  Setup initial calibration tables.
logprint ("Setup initial calibration tables", logfileout='logs/bandpass.log')

#Clear previous models of flux calibrator
default('delmod')
vis=ms_active
otf=True
field='1331+305=3C286'
scr=False
delmod()

#1 setjy:
default('setjy')
vis=ms_active
field='1331+305=3C286'
spw=''
selectdata=False
model='3C286_L.im'
listmodimages=False
scalebychan=True
fluxdensity=-1
standard='Perley-Butler 2013'
usescratch=False         # DJP: Okay to be False in version 4.5.  
setjy()


#2: Initial gain curve, TABLE: gain_curves.g

if os.path.exists('gain_curves.g')==False:
    default(gencal)
    vis=ms_active
    caltable='gain_curves.g'
    caltype='gc'
    spw=''
    antenna=''
    pol=''
    parameter=[]
    gencal()

priorcals=['gain_curves.g']
priorspwmap=[[]]

#3: Antenna position calibration:
if os.path.exists('antposcal.p')==False:
    default(gencal)
    vis=ms_active
    caltable='antposcal.p'
    caltype='antpos'
    spw=''
    antenna=''
    pol=''
    parameter=[]
    gencal()

if os.path.exists('antposcal.p')==True:  # HG (12/04/2015) : include antposcal.p in the priorcals if it exists
  priorcals.append('antposcal.p')
  priorspwmap.append([])


#--------------------------------------------------------------------
#Part III: initial bandpass calibration + rflag
logprint ("Initial delay, gain, BP calibration", logfileout='logs/bandpass.log')

#4: Gain calibration on delay calibrator, TABLE: testdelayinitialgain.g
default('gaincal')
vis=ms_active
caltable='testdelayinitialgain.g'
field='1331+305=3C286'            # Hard-coded to field 2 as this is always 3C286
spw=tst_delay_spw
intent=''
selectdata=False
solint='int'
combine='scan'
preavg=-1.0
refant=refAnt
uvrange=uvr_cal      # Set uvrange to exclude worst of RFI
minblperant= minBL_for_cal
minsnr=3.0
solnorm=False
gaintype='G'
smodel=[]
calmode='p'
append=False
docallib=False
gaintable=priorcals
gainfield=['']
interp=['']
spwmap=[]
parang=False
async=False  # Not deprecated
gaincal()

GainTables=copy.copy(priorcals)
GainTables.append('testdelayinitialgain.g')

#Delay calibration to all spws. TABLE: initialdelay.k
default('gaincal')
vis=ms_active
caltable='initialdelay.k'
field='1331+305=3C286'            # Hard-coded to field 2 as this is always 3C286
spw=''
intent=''
selectdata=False
uvrange=''           # No uvrange for delay calibration, since it exclude antennas.
solint='inf'
combine='scan'
preavg=-1.0
refant=refAnt
minblperant=minBL_for_cal
minsnr=3.0
solnorm=False
gaintype='K'
smodel=[]
calmode='p'
append=False
gaintable=GainTables
gainfield=['']
interp=['']
spwmap=[]
opacity=[]
parang=False
async=False
gaincal()

#specify spw14 to apply delays from there

spwac=14
spwmapdelayarray=np.zeros(15,int)
spwmapdelayarray[0:15]=spwac
spwmapdelay=list(spwmapdelayarray)

GainTables=copy.copy(priorcals)
GainTables.append('initialdelay.k')
SpwMapValues=copy.copy(priorspwmap)
SpwMapValues.append(spwmapdelay)


#5: This second gaincal applies the delays of spw 14? TABLE:initialBPinitialgain.g
default('gaincal')
vis=ms_active
caltable='initialBPinitialgain.g'
field='1331+305=3C286'            # Hard-coded to field 2 as this is always 3C286
spw=tst_bpass_spw
selectdata=False
solint='int'
combine='scan'
preavg=-1.0
refant=refAnt
uvrange=uvr_cal      # Set uvrange to exclude worst of RFI
minblperant=minBL_for_cal
minsnr=3.0
solnorm=False
gaintype='G'
smodel=[]
calmode='p'
append=False
gaintable=GainTables
gainfield=['']
interp=['']
spwmap=SpwMapValues
opacity=[]
parang=False
async=False
gaincal()

BPGainTables=copy.copy(GainTables)            # HG (12/04/2015) : simplifying steps for gaintables and spwmaps
BPGainTables.append('initialBPinitialgain.g')
BPSpwMapValues=copy.copy(SpwMapValues)
BPSpwMapValues.append([])

#6: Bandpass solution: TABLE: initialBPcal.b
default('bandpass')
vis=ms_active
caltable='initialBPcal.b'
field='1331+305=3C286'            # Hard-coded to field 2 as this is always 3C286
#spw='0~14:64~1982'
spw='0~14'           # Solve BP for all channels in spectral line spws.
#spw=tst_bpass_spw     # Only solve in clean windows. 
selectdata=True
uvrange=uvr_cal      # Set uvrange to exclude worst of RFI
solint='inf'
combine='scan'
refant=refAnt
minblperant=minBL_for_cal
minsnr=5.0
solnorm=False       # Must be set to False otherwise solution doesn't apply BP to flagged channels
bandtype='B'
fillgaps=0          # Skip interpolation, set to 0.
interp=['']          # Default is linear interpolation in time, frequency.
append=False
gaintable=BPGainTables
gainfield=['']
spwmap=BPSpwMapValues
gaincurve=False
opacity=[]
parang=False
bandpass()


AllCalTables=copy.copy(GainTables)           # HG (12/04/2015) : simplifying steps for gaintables and spwmaps
AllCalTables.append('initialBPcal.b')
AllCalTables.append('initialBPinitialgain.g')
AllSpwMapValues=copy.copy(SpwMapValues)
AllSpwMapValues.append([])
AllSpwMapValues.append([])

#7: #Apply calibrations to the flux calibrator:
default('applycal')
vis=ms_active
field='1331+305=3C286'            # Hard-coded to field 2 as this is always 3C286
spw=''
intent=''
selectdata=True
gaintable=AllCalTables
interp=['','','','','']         # Default is to interpolate linearly in time & frequency.
spwmap=AllSpwMapValues
gaincurve=False
opacity=[]
parang=False
calwt=False
flagbackup=False
async=False
applycal()

#8: First rflag run 
# This is the new flagging code from XF.  
logprint ("Flagging flux calibrator by Clipping & RFLAG", logfileout='logs/bandpass.log')
default('flagdata')
vis=ms_active
datacolumn='corrected'
field='1331+305=3C286'
spw='14'
mode='clip'
clipminmax=[0,50]
action='apply'
flagbackup=False
flagdata()

#This calculates timedev and freqdev for spw14
default('flagdata')
vis=ms_active
datacolumn='corrected'
field='1331+305=3C286'
spw='14'
scan=''
mode='rflag'
timedev='tdev14_f2.txt'
freqdev='fdev14_f2.txt'
freqdevscale=1.0
timedevscale=1.0
extendflags=False
flagbackup=False
action='calculate'
flagdata()


with open("tdev14_f2.txt","r") as data:
    dictTime=ast.literal_eval(data.read())

with open("fdev14_f2.txt","r") as data:
    dictFreq=ast.literal_eval(data.read())

timenoavg=dictTime["timedev"][0][2]
freqnoavg=dictFreq["freqdev"][0][2]

ff=float(2)
scaling1=[2.4,2.1,1.9,1.3,1.2,1.2,1.1,1.1,1.1,1.1,1.0,1.0,1.0,1.0,1.0]
scaling=np.asarray(scaling1)
sigmacut=8.0
freqd1noavg=scaling*freqnoavg*sigmacut
timed1noavg=scaling*timenoavg*sigmacut

default('flagdata')
vis=ms_active
mode='rflag'
field='1331+305=3C286'
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

# Summary of flagging, after RFLAG+extend (for testing purposes only)
logprint ("Summary of flags after Flagging", logfileout='logs/bandpass.log')
default('flagdata')
vis=ms_active
field='1331+305=3C286'       # Only flagging 3C286, so don't need stats on other sources.
mode='summary'
spw='0~14'
correlation='RR,LL'
spwchan=True
spwcorr=True
basecnt=True
action='calculate'
s_b=flagdata()

flux_flag=s_b['flagged']/s_b['total']

logprint ("Percentage of 3C286 data flagged after flagging in BP module: "+str(flux_flag*100)+'%', logfileout='logs/bandpass.log')

#--------------------------------------------------------------------
#Part IV: second round of bandpass calibration
logprint ("Final delay, gain, BP calibration", logfileout='logs/bandpass.log')

#10:Gain calibration on delay calibrator, TABLE: finaldelayinitialgain.g
default('gaincal')
vis=ms_active
caltable='finaldelayinitialgain.g'
field='1331+305=3C286'            # Hard-coded to field 2 as this is always 3C286
spw=tst_delay_spw
intent=''
selectdata=False
solint='int'
combine='scan'
preavg=-1.0
refant=refAnt
uvrange=uvr_cal      # Set uvrange to exclude worst of RFI
minblperant= minBL_for_cal
minsnr=3.0
solnorm=False
gaintype='G'
smodel=[]
calmode='p'
append=False
docallib=False
gaintable=priorcals
gainfield=['']
interp=['']
spwmap=[]
parang=False
async=False
gaincal()

DelayTables=copy.copy(priorcals)
DelayTables.append('finaldelayinitialgain.g')

#11: Delay calibration to all spws. TABLE: finaldelay.k
default('gaincal')
vis=ms_active
caltable='finaldelay.k'
field='1331+305=3C286'            # Hard-coded to field 2 as this is always 3C286
spw=''
intent=''
selectdata=False
uvrange=''           # No uvrange for delay calibration, since it exclude antennas.
solint='inf'
combine='scan'
preavg=-1.0
refant=refAnt
minblperant=minBL_for_cal
minsnr=3.0
solnorm=False
gaintype='K'
smodel=[]
calmode='p'
append=False
gaintable=DelayTables
gainfield=['']
interp=['']
spwmap=[]
opacity=[]
parang=False
async=False
gaincal()

#specify spw14 to apply delays from there

spwac=14
spwmapdelayarray=np.zeros(15,int)
spwmapdelayarray[0:15]=spwac
spwmapdelay=list(spwmapdelayarray)

GainTables=copy.copy(priorcals)
GainTables.append('finaldelay.k')
SpwMapValues=copy.copy(priorspwmap)
SpwMapValues.append(spwmapdelay)


#12:This second gaincal applies the delays of spw 14. TABLE:finalBPinitialgain.g
default('gaincal')
vis=ms_active
caltable='finalBPinitialgain.g'
field='1331+305=3C286'            # Hard-coded to field 2 as this is always 3C286
spw=tst_bpass_spw
selectdata=False
solint='int'
combine='scan'
preavg=-1.0
refant=refAnt
uvrange=uvr_cal      # Set uvrange to exclude worst of RFI
minblperant=minBL_for_cal
minsnr=3.0
solnorm=False
gaintype='G'
smodel=[]
calmode='p'
append=False
gaintable=GainTables
gainfield=['']
interp=['']
spwmap=SpwMapValues
opacity=[]
parang=False
async=False
gaincal()

BPGainTables=copy.copy(GainTables)          # HG (12/04/2015) : simplifying steps for caltables and spwmap
BPGainTables.append('finalBPinitialgain.g')
BPSpwMapValues=copy.copy(SpwMapValues)
BPSpwMapValues.append([])

#13: Bandpass solution: TABLE: finalBPcal.b
default('bandpass')
vis=ms_active
caltable='finalBPcal.b'
field='1331+305=3C286'            # Hard-coded to field 2 as this is always 3C286
#spw='0~14:64~1982'
spw='0~14'           # Solve BP for all channels in spectral line spws.
#spw=tst_bpass_spw
selectdata=True
uvrange=uvr_cal      # Set uvrange to exclude worst of RFI
solint='inf'
combine='scan'
refant=refAnt
minblperant=minBL_for_cal
minsnr=5.0
solnorm=False       # Must be set to False otherwise solution doesn't apply BP to flagged channels
bandtype='B'
fillgaps=0          # No frequency interpolation desired.
interp=['']          # Default linear interpolation in time, frequency.
append=False
gaintable=BPGainTables
gainfield=['']
spwmap=BPSpwMapValues
gaincurve=False
opacity=[]
parang=False
bandpass()


AllCalTables=copy.copy(GainTables)
AllCalTables.append('finalBPcal.b')
AllCalTables.append('finalBPinitialgain.g')
AllSpwMapValues=copy.copy(SpwMapValues)
AllSpwMapValues.append([])
AllSpwMapValues.append([])

#15:
#Apply calibrations to the flux calibrator:
default('applycal')
vis=ms_active
field='1331+305=3C286'            # Hard-coded to field 2 as this is always 3C286
spw=''
intent=''
selectdata=True
gaintable=AllCalTables
interp=['','','','','']         # Default is to interpolate linearly in time & frequency.
spwmap=AllSpwMapValues
gaincurve=False
opacity=[]
parang=False
calwt=False
flagbackup=False
async=False
applycal()

# Backup finalBPcal table

os.system('cp -r finalBPcal.b finalBPcal_backup.b')

# Save flags
logprint ("Saving flags", logfileout='logs/bandpass.log')


default('flagmanager')
vis=ms_active
mode='save'
versionname='fluxcal_flags'
comment='Flux calibrator flags saved after application'
merge='replace'
flagmanager()
logprint ("Flag column saved to "+versionname, logfileout='logs/bandpass.log')


logprint ("Finished CHILES_pipe_bandpass.py", logfileout='logs/bandpass.log')
time_list=runtiming('bandpass', 'end')

pipeline_save()
