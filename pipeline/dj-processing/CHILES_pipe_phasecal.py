# CHILES_pipe_phasecal.py
# This module of the CHILES pipeline follows the "bandpass" module using the BP 
# and flux calibration from that task to solve for the complex gain (amp & phase)
# as a function of time using the phase calibrator.  This will involve doing an
# initial solution, running an RFLAG with extend, then getting a final solution
# ready to apply to the observations of the deepfield itself.
# 9/7/15 DJP
# HG fixed a number of bugs and explicitly sets caltable and spw lists correctly.
# 12/7/15 DJP
# 1/27/16 DJP: Set extendpols=False in RFLAG step.
# 2/9/16 DJP: Removed many flag summaries, limit to phase calibrator.
# 2/9/16 DJP: Moved applycal for target to "target" module.
# 2/11/16 DJP: Make cloud plots without any averaging.
# 2/15/16 DJP: Changed ff value to a float.
# 2/18/16 DJP: Make diagnostic plots with averaged, split data
# 2/18/16 DJP: Make plot of amp v. frequency for 3C286 averaged over time & baseline from UV data.
# 2/18/16 DJP: If re-running phasecal module, need to delmod on phase calibrator.
# 2/19/16 DJP: Make 2 UVSPEC plots (one with full range, one with zoom).  Changed averaging.  Removed finalflux.gcal plot.
# 4/8/16 DJP: Set minsnr for calibration solution to 8.
# 6/16/16 DJP: Removing images from phase calibrator from diagnostic plots, but including Amp/Phase vs. time.
# 6/22/16 DJP:  Setting minsnr to original pipeline values.
# 9/21/16 DJP:  Changed channel range for tst_gain_spw (only excluding 50 edge channels).  Moved flagmanager to end.
# 10/28/16 DJP: Added backup of finalamp.gcal
# 4/22/18 DJP: Changing flagging and split to oldsplit
# 8/29/18 DJP: Changing field numbers to field names.
# 10/8/18 DJP: Including flagging percentage vs. uvdist, use flags instead of masks.
# 12/19/18 DJP:  Moved mask flags to initial module


logprint ("Starting CHILES_pipe_phasecal.py", logfileout='logs/phasecal.log')
time_list=runtiming('phase', 'start')

# Step 1:  Set default parameters (to match previous modules), and clean up old files:
import copy
import numpy as np
import pylab as pylab
import re as re
import sys

#Remove old files from previous attempts using rmtables
#Clear model of phase calibrator if module previously run.
if os.path.exists('initialphase.gcal'):
    rmtables(tablenames='initialphase.gcal')
if os.path.exists('initialamp.gcal'):
    rmtables(tablenames='initialamp.gcal')
if os.path.exists('initialflux.gcal'):
    rmtables(tablenames='initialflux.gcal')
if os.path.exists('finalphase_int.gcal'):
    rmtables(tablenames='finalphase_int.gcal')
if os.path.exists('finalphase_scan.gcal'):
    rmtables(tablenames='finalphase_scan.gcal')
if os.path.exists('finalamp.gcal'):
    rmtables(tablenames='finalamp.gcal')
if os.path.exists('finalflux.gcal'):
    rmtables(tablenames='finalflux.gcal')
    # If caltables exist, then delmod must be run.
    default('delmod')
    vis=ms_active
    otf=True
    field='J0943-0819'  # Hard-coded for phase calibrator
    scr=False
    delmod()
os.system("rm -rf images/phasecalibrator_spw*.*")


# The following parameters are set in the initial pipeline script by default or user input.
##Set minimum # of baselines need for a solution to 8, based on experience
minBL_for_cal=8

##Set uvrange to apply in order to optimally exclude RFI without flagging:
uvr_cal='>1500m'

#Set prior cals including gain, delay, and BP calibration
# HG : adding the spwmap for prior cals. & adding the conditional statements for antposcal.p

spwac=14
spwmapdelayarray=np.zeros(15,int)
spwmapdelayarray[0:15]=spwac
spwmapdelay=list(spwmapdelayarray)

# HG: if no antenna position changes, then table not generated.
if os.path.exists('antposcal.p')==True:
  priorcals=['antposcal.p','gain_curves.g','finaldelay.k','finalBPcal.b']
  priorspwmap=[[],[],spwmapdelay,[]]

if os.path.exists('antposcal.p')==False:
  priorcals=['gain_curves.g','finaldelay.k','finalBPcal.b']
  priorspwmap=[[],spwmapdelay,[]]


# Set channel/spw range to use for calibrations (based on BP code)
tst_gain_spw=tst_bpass_spw


# This module is hard-coded for CHILES, so:
# field='J0943-0819' is J0943-0819, phase calibrator
# Field='1331+305=3C286', flux & bandpass calibrator
# Field='deepfield', the target


  
# Step 2:  gaincal on both calibrators
# Need short solint = "int" (for calibrators)
# Long solint = "inf" (to apply to target) not needed until final iteration.

logprint ("Running initial gaincal", logfileout='logs/phasecal.log')

default('gaincal')
vis=ms_active
caltable='initialphase.gcal'
field='J0943-0819,1331+305=3C286'
spw=tst_gain_spw
intent=''
selectdata=False
solint='int'
combine=''
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
spwmap=priorspwmap     # In previous version this was [].  These spw's correspond to priorcals.
parang=False
async=False
gaincal()
  
# Step 3: Now use to derive amp & phase solutions
# This step is used to derive solutions with solint='inf' (averaging over scans only)

logprint ("Deriving initial amp & phase solutions", logfileout='logs/phasecal.log')

GainTables=copy.copy(priorcals)
GainTables.append('initialphase.gcal')
SpwMapValues=copy.copy(priorspwmap)
SpwMapValues.append([])


default('gaincal')
vis=ms_active
caltable='initialamp.gcal'
field='J0943-0819,1331+305=3C286'
spw=tst_gain_spw
intent=''
selectdata=False
solint='inf'
combine=''
preavg=-1.0
refant=refAnt
uvrange=uvr_cal      # Set uvrange to exclude worst of RFI
minblperant= minBL_for_cal
minsnr=3.0
solnorm=False
gaintype='G'
smodel=[]
calmode='ap'         # Solve for both amp & phase
append=False
docallib=False
gaintable=GainTables
gainfield=['']
interp=['']
spwmap=SpwMapValues  # In previous version this was [].  These spw's correspond to GainTables.
parang=False
async=False
gaincal()

# Step 4:  Bootstrap fluxes using code from EVLA_pipe_fluxboot.
fluxscale_output=msname.rstrip('ms')+'fluxdensities'
fluxcalfields=flux_field_select_string
if os.path.exists(fluxscale_output):
    os.system('rm -rf '+fluxscale_output)

logprint ("Initial flux bootstrapping", logfileout='logs/phasecal.log')
logprint ("Flux densities will be written to "+fluxscale_output, logfileout='logs/phasecal.log')

casalog.setlogfile(fluxscale_output)

default('fluxscale')
vis=ms_active
caltable='initialamp.gcal'
fluxtable='initialflux.gcal'
reference='1331+305=3C286'      # Reference field is always 3C286
transfer=['J0943-0819']
listfile=''
append=False
refspwmap=[-1]        
incremental=True   
fitorder=1

try:
    fluxscale_result=fluxscale()

    casalog.setlogfile(maincasalog)


    logprint ("Fitting data with power law", logfileout='logs/phasecal.log')


#
# the variable center_frequencies should already have been filled out
# with the reference frequencies of the spectral window table
#

    fitfunc = lambda p, x: p[0] + p[1] * x
    errfunc = lambda p, x, y, err: (y - fitfunc(p, x)) / err

    try:
        ff = open(fluxscale_output, 'r')
    except IOError as err:
        logprint (fluxscale_output+" doesn't exist, error: "+err.filename, logfileout='logs/phasecal.log')

# looking for lines like:
#2012-03-09 21:30:23     INFO    fluxscale::::    Flux density for J1717-3342 in SpW=3 is: 1.94158 +/- 0.0123058 (SNR = 157.777, N= 34)
# sometimes they look like:
#2012-03-09 21:30:23     INFO    fluxscale::::    Flux density for J1717-3342 in SpW=0 is:  INSUFFICIENT DATA 
# so watch for that.

    sources = []
    flux_densities = []
    spws = []

#Find the field_ids in the dictionary returned from the CASA task fluxscale
    dictkeys = fluxscale_result.keys()
    keys_to_remove = ['freq', 'spwName', 'spwID']
    dictkeys = [field_id for field_id in dictkeys if field_id not in keys_to_remove]

    logprint("Finding field_ids returned from fluxscale.", logfileout='logs/phasecal.log')

    for field_id in dictkeys:
        sourcename = fluxscale_result[field_id]['fieldName']
        secondary_keys = fluxscale_result[field_id].keys()
        secondary_keys_to_remove=['fitRefFreq', 'spidxerr', 'spidx', 'fitFluxd', 'fieldName', 'fitFluxdErr']
        spwkeys = [spw_id for spw_id in secondary_keys if spw_id not in secondary_keys_to_remove]

        for spw_id in spwkeys:
            flux_d = list(fluxscale_result[field_id][spw_id]['fluxd'])
            flux_d_err = list(fluxscale_result[field_id][spw_id]['fluxdErr'])
            #spwslist  = list(int(spw_id))

            #flux_d = list(fluxscale_result[field_id]['fluxd'])
            #flux_d_err = list(fluxscale_result[field_id]['fluxdErr'])
            #spwslist  = list(fluxscale_result['spwID'])

            for i in range(0,len(flux_d)):
                if (flux_d[i] != -1.0 and flux_d[i] != 0.0):
                    sources.append(sourcename)
                    flux_densities.append([float(flux_d[i]), float(flux_d_err[i])])
                    spws.append(int(spw_id))

    logprint("Finding sources returned from fluxscale.", logfileout='logs/phasecal.log')

    ii = 0
    unique_sources = list(np.unique(sources))
    results = []
    for source in unique_sources:
        indices = []
        for ii in range(len(sources)):
            if (sources[ii] == source):
                indices.append(ii)
        unique_bands = ['L']
        band=unique_bands[0]
        lfreqs = []
        lfds = []
        lerrs = []
        uspws = []
        for ii in range(len(indices)):
                lfreqs.append(log10(center_frequencies[spws[indices[ii]]]))
                lfds.append(log10(flux_densities[indices[ii]][0]))
                lerrs.append(log10(e) * flux_densities[indices[ii]][1]/flux_densities[indices[ii]][0])
                uspws.append(spws[indices[ii]])
# if we didn't care about the errors on the data or the fit coefficients, just:
#       coefficients = np.polyfit(lfreqs, lfds, 1)
# or, if we ever get to numpy 1.7.x, for weighted fit, and returning
# covariance matrix, do:
#       ...
#       weights = []
#       weight_sum = 0.0
#       for ii in range(len(lfreqs)):
#           weights.append(1.0 / (lerrs[ii]*lerrs[ii]))
#           weight_sum += weights[ii]
#       for ii in range(len(weights)):
#           weights[ii] /= weight_sum
#       coefficients = np.polyfit(lfreqs, lfds, 1, w=weights, cov=True)
# but, for now, use the full scipy.optimize.leastsq route...
#
# actually, after a lot of testing, np.polyfit does not return a global
# minimum solution.  sticking with leastsq (modified as below to get the
# proper errors), or once we get a modern enough version of scipy, moving
# to curve_fit, is better.
#

        if len(lfds) < 2:
            aa = lfds[0]
            bb = 0.0
            SNR = 0.0
        else:
            alfds = scp.array(lfds)
            alerrs = scp.array(lerrs)
            alfreqs = scp.array(lfreqs)
            pinit = [0.0, 0.0]
            fit_out = scpo.leastsq(errfunc, pinit, args=(alfreqs, alfds, alerrs), full_output=1)
            pfinal = fit_out[0]
            covar = fit_out[1]
            aa = pfinal[0]
            bb = pfinal[1]
#
# the fit is of the form:
#     log(S) = a + b * log(f)
# with a = pfinal[0] and b = pfinal[1].  the errors on the coefficients are
# sqrt(covar[i][i]*residual_variance) with the residual covariance calculated
# as below (it's like the reduced chi squared without dividing out the errors).
# see the scipy.optimize.leastsq documentation and 
# http://stackoverflow.com/questions/14854339/in-scipy-how-and-why-does-curve-fit-calculate-the-covariance-of-the-parameter-es
#
            summed_error = 0.0
            for ii in range(len(alfds)):
                model = aa + bb*alfreqs[ii]
                residual = (model - alfds[ii]) * (model - alfds[ii])
                summed_error += residual
            residual_variance = summed_error / (len(alfds) - 2)
            SNR = fabs(bb) / sqrt(covar[1][1] * residual_variance)

#
# take as the reference frequency the lowest one.  (this shouldn't matter,
# in principle).
#

        logprint("Setting flux density, ref. freq, and spix", logfileout='logs/phasecal.log')
        
        reffreq = 10.0**lfreqs[0]/1.0e9
        fluxdensity = 10.0**(aa + bb*lfreqs[0])
        spix = bb
        results.append([ source, uspws, fluxdensity, spix, SNR, reffreq ])
        logprint(source + ' ' + ', '+ band + ' fitted spectral index & SNR = ' + str(spix) + ' ' + str(SNR), logfileout='logs/phasecal.log')
        logprint("Frequency, data, error, and fitted data:", logfileout='logs/phasecal.log')
        for ii in range(len(lfreqs)):
            SS = fluxdensity * (10.0**lfreqs[ii]/reffreq/1.0e9)**spix
            fderr = lerrs[ii]*(10**lfds[ii])/log10(e)
            logprint('    '+str(10.0**lfreqs[ii]/1.0e9)+'  '+ str(10.0**lfds[ii])+'  '+str(fderr)+'  '+str(SS), logfileout='logs/phasecal.log')
    
    
    logprint ("Setting power-law fit in the model column", logfileout='logs/phasecal.log')

    for result in results:
        for spw_i in result[1]:
#
# here, check on SNR, but don't do this yet, until we know what typical SNRs are
#
#           if result[4] > SNRlimit:
            logprint('Running setjy on spw '+str(spw_i), logfileout='logs/phasecal.log')
            default('setjy')
            vis=ms_active
            field = str(result[0])
            #spw = ','.join(["%s" % ii for ii in result[1]])
            spw = str(spw_i)
            selectdata=False
            scalebychan=True
            standard='manual'
            fluxdensity = [ result[2], 0, 0, 0 ]
            spix = result[3]
            reffreq = str(result[5])+'GHz'
            usescratch=False
            try:
                setjy()
                if (abs(spix) > 5.0):
                    QA2_fluxboot='Fail'
            except:
                logprint("Unable to complete flux scaling operation for field "+str(field)+", spw "+str(spw), logfileout='logs/phasecal.log')
except:
    logprint("A problem was detected while running fluxscale.  Please review the CASA log.", logfileout='logs/phasecal.log')


# Step 5: applycal to flux & phase cal

logprint ("Initial applycal", logfileout='logs/phasecal.log')

AllCalTables=copy.copy(GainTables)        # Using both amp.gcal and flux.gcal tables.
AllCalTables.append('initialamp.gcal')
AllCalTables.append('initialflux.gcal')
AllSpwMapValues=copy.copy(SpwMapValues)
AllSpwMapValues.append([])
AllSpwMapValues.append([])

# HG: set gainfield appropriately depending if antposcal.p exists or not.  
if os.path.exists('antposcal.p')==True:  
  PhaseFields=['','','1331+305=3C286','1331+305=3C286','J0943-0819','J0943-0819','J0943-0819']
  FluxFields =['','','1331+305=3C286','1331+305=3C286','1331+305=3C286','1331+305=3C286','1331+305=3C286']

if os.path.exists('antposcal.p')==False:
  PhaseFields=['','1331+305=3C286','1331+305=3C286','J0943-0819','J0943-0819','J0943-0819']
  FluxFields =['','1331+305=3C286','1331+305=3C286','1331+305=3C286','1331+305=3C286','1331+305=3C286']


default('applycal')
vis=ms_active
field='J0943-0819'            # Apply one calibration to phase cal
spw=''
intent=''
selectdata=True
gaintable=AllCalTables
gainfield=PhaseFields
interp=['']             # Default interpolation is linear in time & frequency
spwmap=AllSpwMapValues  # In previous version this was [].  These spw's correspond to AllCalTables.
gaincurve=False
opacity=[]
parang=False
calwt=False
flagbackup=False
async=False
applycal()

default('applycal')
vis=ms_active
field='1331+305=3C286'            # Apply same calibration to flux cal
spw=''
intent=''
selectdata=True
gaintable=AllCalTables
gainfield=FluxFields
interp=['']             # Default interpolation is linear in time & frequency
spwmap=AllSpwMapValues  # In previous version this was [].  These spw's correspond to AllCalTables.
gaincurve=False
opacity=[]
parang=False
calwt=False
flagbackup=False
async=False
applycal()

# Step 6: run RFLAG 
# New Flagging routines from XF
logprint ("Initial Clipping & RFLAG of Phase Calibrator", logfileout='logs/phasecal.log')
default('flagdata')
vis=ms_active
datacolumn='corrected'
field='J0943-0819'
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
field='J0943-0819'
spw='14'
scan=''
mode='rflag'
timedev='tdev14_f0.txt'
freqdev='fdev14_f0.txt'
freqdevscale=1.0
timedevscale=1.0
extendflags=False
action='calculate'
flagdata()


with open("tdev14_f0.txt","r") as data:
    dictTime=ast.literal_eval(data.read())

with open("fdev14_f0.txt","r") as data:
    dictFreq=ast.literal_eval(data.read())

timenoavg=dictTime["timedev"][0][2]
freqnoavg=dictFreq["freqdev"][0][2]

ff=float(0)
scaling1=[2.4,2.1,1.9,1.3,1.2,1.2,1.1,1.1,1.1,1.1,1.0,1.0,1.0,1.0,1.0]
scaling=np.asarray(scaling1)
sigmacut=8.0
freqd1noavg=scaling*freqnoavg*sigmacut
timed1noavg=scaling*timenoavg*sigmacut

default('flagdata')
vis=ms_active
mode='rflag'
field='J0943-0819'
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
logprint ("Summary of flags after Flagging", logfileout='logs/phasecal.log')
default('flagdata')
vis=ms_active
mode='summary'
spw='0~14'
field='J0943-0819'     # Only phase calibrator getting flagged so only source to check.
correlation='RR,LL'
spwchan=True
spwcorr=True
basecnt = True
action='calculate'
s_p=flagdata()

phase_flag=s_p['flagged']/s_p['total']

logprint ("Percentage of all data flagged after flagging in phasecal module: "+str(phase_flag*100)+'%', logfileout='logs/phasecal.log')

# Step 7: repeat gaincal 
# Short solint = "int" (length of individual calibrator scan)

logprint ("Running final gaincal", logfileout='logs/phasecal.log')

default('gaincal')
vis=ms_active
caltable='finalphase_int.gcal'
field='J0943-0819,1331+305=3C286'
spw=tst_gain_spw
intent=''
selectdata=False
solint='int'
combine=''
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
spwmap=priorspwmap # In previous version this was [].  These spw's correspond to priorcals.
parang=False
async=False
gaincal()

default('gaincal')
vis=ms_active
caltable='finalphase_scan.gcal'
field='J0943-0819,1331+305=3C286'
spw=tst_gain_spw
intent=''
selectdata=False
solint='inf'
combine=''
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
spwmap=priorspwmap  # In previous version this was [].  These spw's correspond to priorcals.
parang=False
async=False
gaincal()

logprint ("Deriving final amp & phase solutions", logfileout='logs/phasecal.log')

GainTables=copy.copy(priorcals)
GainTables.append('finalphase_int.gcal')
SpwMapValues=copy.copy(priorspwmap)
SpwMapValues.append([])

default('gaincal')
vis=ms_active
caltable='finalamp.gcal'
field='J0943-0819,1331+305=3C286'
spw=tst_gain_spw
intent=''
selectdata=False
solint='inf'
combine=''
preavg=-1.0
refant=refAnt
uvrange=uvr_cal      # Set uvrange to exclude worst of RFI
minblperant= minBL_for_cal
minsnr=3.0
solnorm=False
gaintype='G'
smodel=[]
calmode='ap'         # Solve for both amp & phase
append=False
docallib=False
gaintable=GainTables
gainfield=['']
interp=['']
spwmap=SpwMapValues  # In previous version this was [].  These spw's correspond to GainTables.
parang=False
async=False
gaincal()

# Step 8: repeat bootstrapping
os.system('rm -rf '+fluxscale_output)
logprint ("Final flux bootstrapping", logfileout='logs/phasecal.log')
logprint ("Flux densities will be written to "+fluxscale_output, logfileout='logs/phasecal.log')

#Clear previous models of phase calibrator
default('delmod')
vis=ms_active
otf=True
field='J0943-0819'  # Hard-coded for phase calibrator
scr=False
delmod()

casalog.setlogfile(fluxscale_output)

default('fluxscale')
vis=ms_active
caltable='finalamp.gcal'
fluxtable='finalflux.gcal'
reference='1331+305=3C286'
transfer=['J0943-0819']
listfile=''
append=False
refspwmap=[-1]        
incremental=True
fitorder=1

try:
    fluxscale_result=fluxscale()

    casalog.setlogfile(maincasalog)


    logprint ("Fitting data with power law", logfileout='logs/phasecal.log')


#
# the variable center_frequencies should already have been filled out
# with the reference frequencies of the spectral window table
#

    fitfunc = lambda p, x: p[0] + p[1] * x
    errfunc = lambda p, x, y, err: (y - fitfunc(p, x)) / err

    try:
        ff = open(fluxscale_output, 'r')
    except IOError as err:
        logprint (fluxscale_output+" doesn't exist, error: "+err.filename, logfileout='logs/phasecal.log')

# looking for lines like:
#2012-03-09 21:30:23     INFO    fluxscale::::    Flux density for J1717-3342 in SpW=3 is: 1.94158 +/- 0.0123058 (SNR = 157.777, N= 34)
# sometimes they look like:
#2012-03-09 21:30:23     INFO    fluxscale::::    Flux density for J1717-3342 in SpW=0 is:  INSUFFICIENT DATA 
# so watch for that.

    sources = []
    flux_densities = []
    spws = []

#Find the field_ids in the dictionary returned from the CASA task fluxscale
    dictkeys = fluxscale_result.keys()
    keys_to_remove = ['freq', 'spwName', 'spwID']
    dictkeys = [field_id for field_id in dictkeys if field_id not in keys_to_remove]

    logprint("Finding field_ids returned from fluxscale.", logfileout='logs/phasecal.log')

    for field_id in dictkeys:
        sourcename = fluxscale_result[field_id]['fieldName']
        secondary_keys = fluxscale_result[field_id].keys()
        secondary_keys_to_remove=['fitRefFreq', 'spidxerr', 'spidx', 'fitFluxd', 'fieldName', 'fitFluxdErr']
        spwkeys = [spw_id for spw_id in secondary_keys if spw_id not in secondary_keys_to_remove]

        for spw_id in spwkeys:
            flux_d = list(fluxscale_result[field_id][spw_id]['fluxd'])
            flux_d_err = list(fluxscale_result[field_id][spw_id]['fluxdErr'])
            #spwslist  = list(int(spw_id))

            #flux_d = list(fluxscale_result[field_id]['fluxd'])
            #flux_d_err = list(fluxscale_result[field_id]['fluxdErr'])
            #spwslist  = list(fluxscale_result['spwID'])

            for i in range(0,len(flux_d)):
                if (flux_d[i] != -1.0 and flux_d[i] != 0.0):
                    sources.append(sourcename)
                    flux_densities.append([float(flux_d[i]), float(flux_d_err[i])])
                    spws.append(int(spw_id))

    logprint("Finding sources returned from fluxscale.", logfileout='logs/phasecal.log')

    ii = 0
    unique_sources = list(np.unique(sources))
    results = []
    for source in unique_sources:
        indices = []
        for ii in range(len(sources)):
            if (sources[ii] == source):
                indices.append(ii)
        unique_bands = ['L']
        band=unique_bands[0]  # Needs to be defined for a logprint statement.  Meaningless otherwise
        lfreqs = []
        lfds = []
        lerrs = []
        uspws = []
        for ii in range(len(indices)):
                lfreqs.append(log10(center_frequencies[spws[indices[ii]]]))
                lfds.append(log10(flux_densities[indices[ii]][0]))
                lerrs.append(log10(e) * flux_densities[indices[ii]][1]/flux_densities[indices[ii]][0])
                uspws.append(spws[indices[ii]])
# if we didn't care about the errors on the data or the fit coefficients, just:
#       coefficients = np.polyfit(lfreqs, lfds, 1)
# or, if we ever get to numpy 1.7.x, for weighted fit, and returning
# covariance matrix, do:
#       ...
#       weights = []
#       weight_sum = 0.0
#       for ii in range(len(lfreqs)):
#           weights.append(1.0 / (lerrs[ii]*lerrs[ii]))
#           weight_sum += weights[ii]
#       for ii in range(len(weights)):
#           weights[ii] /= weight_sum
#       coefficients = np.polyfit(lfreqs, lfds, 1, w=weights, cov=True)
# but, for now, use the full scipy.optimize.leastsq route...
#
# actually, after a lot of testing, np.polyfit does not return a global
# minimum solution.  sticking with leastsq (modified as below to get the
# proper errors), or once we get a modern enough version of scipy, moving
# to curve_fit, is better.
#

        if len(lfds) < 2:
            aa = lfds[0]
            bb = 0.0
            SNR = 0.0
        else:
            alfds = scp.array(lfds)
            alerrs = scp.array(lerrs)
            alfreqs = scp.array(lfreqs)
            pinit = [0.0, 0.0]
            fit_out = scpo.leastsq(errfunc, pinit, args=(alfreqs, alfds, alerrs), full_output=1)
            pfinal = fit_out[0]
            covar = fit_out[1]
            aa = pfinal[0]
            bb = pfinal[1]
#
# the fit is of the form:
#     log(S) = a + b * log(f)
# with a = pfinal[0] and b = pfinal[1].  the errors on the coefficients are
# sqrt(covar[i][i]*residual_variance) with the residual covariance calculated
# as below (it's like the reduced chi squared without dividing out the errors).
# see the scipy.optimize.leastsq documentation and 
# http://stackoverflow.com/questions/14854339/in-scipy-how-and-why-does-curve-fit-calculate-the-covariance-of-the-parameter-es
#
            summed_error = 0.0
            for ii in range(len(alfds)):
                model = aa + bb*alfreqs[ii]
                residual = (model - alfds[ii]) * (model - alfds[ii])
                summed_error += residual
            residual_variance = summed_error / (len(alfds) - 2)
            SNR = fabs(bb) / sqrt(covar[1][1] * residual_variance)

#
# take as the reference frequency the lowest one.  (this shouldn't matter,
# in principle).
#

        logprint("Setting flux density, ref. freq, and spix", logfileout='logs/phasecal.log')
        
        reffreq = 10.0**lfreqs[0]/1.0e9
        fluxdensity = 10.0**(aa + bb*lfreqs[0])
        spix = bb
        results.append([ source, uspws, fluxdensity, spix, SNR, reffreq ])
        logprint(source + ' ' + ', '+ band + ' fitted spectral index & SNR = ' + str(spix) + ' ' + str(SNR), logfileout='logs/phasecal.log')
        logprint("Frequency, data, error, and fitted data:", logfileout='logs/phasecal.log')
        for ii in range(len(lfreqs)):
            SS = fluxdensity * (10.0**lfreqs[ii]/reffreq/1.0e9)**spix
            fderr = lerrs[ii]*(10**lfds[ii])/log10(e)
            logprint('    '+str(10.0**lfreqs[ii]/1.0e9)+'  '+ str(10.0**lfds[ii])+'  '+str(fderr)+'  '+str(SS), logfileout='logs/phasecal.log')
    
    
    logprint ("Setting power-law fit in the model column", logfileout='logs/phasecal.log')

    for result in results:
        for spw_i in result[1]:
#
# here, check on SNR, but don't do this yet, until we know what typical SNRs are
#
#           if result[4] > SNRlimit:
            logprint('Running setjy on spw '+str(spw_i), logfileout='logs/phasecal.log')
            default('setjy')
            vis=ms_active
            field = str(result[0])
            #spw = ','.join(["%s" % ii for ii in result[1]])
            spw = str(spw_i)
            selectdata=False
            scalebychan=True
            standard='manual'
            fluxdensity = [ result[2], 0, 0, 0 ]
            spix = result[3]
            reffreq = str(result[5])+'GHz'
            usescratch=False
            try:
                setjy()
                if (abs(spix) > 5.0):
                    QA2_fluxboot='Fail'
            except:
                logprint("Unable to complete flux scaling operation for field "+str(field)+", spw "+str(spw), logfileout='logs/phasecal.log')
except:
    logprint("A problem was detected while running fluxscale.  Please review the CASA log.", logfileout='logs/phasecal.log')


# Step 9: do final applycal to all fields

logprint ("Running final applycal", logfileout='logs/phasecal.log')

AllCalTables=copy.copy(GainTables)      # using both amp.gcal & flux.gcal tables
AllCalTables.append('finalamp.gcal')
AllCalTables.append('finalflux.gcal')
AllSpwMapValues=copy.copy(SpwMapValues)
AllSpwMapValues.append([])
AllSpwMapValues.append([])

# Backup finalamp.gcal table
os.system('cp finalamp.gcal finalamp_backup.gcal')

# HG: set gainfield appropriately depending if antposcal.p exists or not.  
if os.path.exists('antposcal.p')==True:  
  PhaseFields=['','','1331+305=3C286','1331+305=3C286','J0943-0819','J0943-0819','J0943-0819']
  FluxFields =['','','1331+305=3C286','1331+305=3C286','1331+305=3C286','1331+305=3C286','1331+305=3C286']

if os.path.exists('antposcal.p')==False:
  PhaseFields=['','1331+305=3C286','1331+305=3C286','J0943-0819','J0943-0819','J0943-0819']
  FluxFields =['','1331+305=3C286','1331+305=3C286','1331+305=3C286','1331+305=3C286','1331+305=3C286']


default('applycal')
vis=ms_active
field='J0943-0819'            # Apply final calibration to phase cal
spw=''
intent=''
selectdata=True
gaintable=AllCalTables
gainfield=PhaseFields
interp=['']
spwmap=AllSpwMapValues   # Was [] in previous version, now corresponds to AllCalTables
gaincurve=False
opacity=[]
parang=False
calwt=False
flagbackup=False
async=False
applycal()

default('applycal')
vis=ms_active
field='1331+305=3C286'            # Apply same calibration to phase cal
spw=''
intent=''
selectdata=True
gaintable=AllCalTables
gainfield=FluxFields
interp=['']
spwmap=AllSpwMapValues  # Was [] in previous version, now corresponds to AllCalTables
gaincurve=False
opacity=[]
parang=False
calwt=False
flagbackup=False
async=False
applycal()

# Save flags
logprint ("Saving flags", logfileout='logs/phasecal.log')


default('flagmanager')
vis=ms_active
mode='save'
versionname='phasecal_flags'
comment='Phase Calibrator flags saved after application'
merge='replace'
flagmanager()
logprint ("Flag column saved to "+versionname, logfileout='logs/phasecal.log')


logprint ("Finished CHILES_pipe_phasecal.py", logfileout='logs/phasecal.log')
time_list=runtiming('phase', 'end')

pipeline_save()
