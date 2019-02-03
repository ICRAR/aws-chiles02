# CHILES_pipe_QA.py
# This module is run after the target module in the CHILES pipeline and makes some
# small test cubes to verify that the pipeline calibration and flagging is of
# sufficient quality.  
# 3/3/16 DJP
# 6/28/16, DJP:  Reducing number of iterations to 100 from 1000.  
#                Testing splitting data first. 
# 9/21/16 DJP:  Reduced number of iterations to 1.  As with original code, but running on each spw separately.
# 12/8/16 DJP: Added try, except to spw loop in case one is entirely flagged.
# 12/8/16 DJP: Calculated, print noise for each spectrum.  Changed output layout.
# 3/5/18 DJP: Changed clean to tclean, imaging in topocentric frequency to help with RFI identification.
# 4/22/18 DJP: Changing split to oldsplit
# 11/6/18 DJP: Changed name from testcubes to QA.  Does all imaging/plotting for QA purposes. 
# 12/19/18 DJP:  Including plots showing what's been flagged by masks. 
# 01/11/19 DJP:  Tried running tclean with parallel=True using mpicasa - didn't work for cubes
# 01/13/19 DJP:  Removed parallel=True.  May split QA module into QA1 and QA2 depending on run times
# 01/18/19 DJP:  Had found bug from mpicasa run.  Trying "parallel=True" again.
# 01/21/19 DJP:  Make iterations=2048 for cubes.  Designed to run with mpicasa (should work okay in casa too).

  

logprint ("Starting CHILES_pipe_QA.py", logfileout='logs/QA.log')
time_list=runtiming('QA', 'start')

# If running this module right after starting casa, must run CHILES_pipe_restore first.
# Step 1, load needed functions.
import copy
import numpy as np
import pylab as pylab
import re as re
import sys

# Make plot of short baseline of phase calibrator
# Make plots showing effect of masking RFI on phase calibrator
seq=range(15)
for ii in seq:
    print('Plot Amplitude vs. Frequency for Spw '+str(ii))
    default('plotms')
    vis=ms_active
    field='J0943-0819'        # Only for phase calibrator
    xaxis='freq'
    yaxis='amp'
    ydatacolumn='data'
    averagedata=True
    averagetime='16'
    spw=str(ii)
    uvrange='1500~1600m'
    iteraxis='spw'
    showlegend=False
    title='Flagged Phase Calibrator Data for Spw'+str(ii)+': 1500-1600m'
    showgui=False
    clearplots=True
    customsymbol=True
    customflaggedsymbol=True
    flaggedsymbolshape='autoscaling'
    plotfile='flag1500m.png'
    plotms()

os.system('mv flag*.png plots')

# Make plots for Bandpass QA:
#--------------------------------------------------------------------
#Part V: Data inspection
logprint ("Create data inspection plots for Bandpass module", logfileout='logs/QA.log')

# Make plot of flagging statistics
# s_b is the output of flagdata run (above)
# Get information for flagging percentage vs. uvdistance
#gantdata = get_antenna_data(ms_active)
#create adictionary with flagging info
#base_dict = create_baseline_dict(ms_active, gantdata)
#gantdata and base_dict are already in the initial module so no need to retrieve that information again.
#match flagging data to dictionary entry
datamatch = flag_match_baseline(s_b['baseline'], base_dict)
#bin the statistics
binned_stats = bin_statistics(datamatch, 'B', 25)  # 25 is the number of uvdist bins such that there is minimal error in uvdist.

#Plot flagging % vs. uvdist
### Plot the Data
barwidth = binned_stats[0][1]
totflagged = 'BP Flagging: '+ str(flux_flag*100) + '% Data Flagged'
pylab.close()
pylab.bar(binned_stats[0],binned_stats[1], width=barwidth, color='grey', align='edge')
pylab.title(totflagged)
pylab.grid()
pylab.ylabel('flagged data [%]')
pylab.xlabel('average UV distance [m]')
pylab.savefig('bp_flag_uvdist.png')
os.system("mv bp_flag_uvdist.png plots/.") 

# Make plot of percentage of data flagged as a function of channel (for both correlations combined):
flag_frac=[]
ct=-1
chan=[]
freq=[]
flagged=[]
totals=[]
# Extract frequency of first channel of spw=0 from listobs output
nu0=reference_frequencies[0]/1.e6 #get reference frequency in MHz
dnu=0.015625 # channel width in MHz
for s in range(15):
    for c in range(2048):
        ct+=1
        chan.append(ct)
        freq.append(nu0+dnu*ct)
        flagged.append(s_b['spw:channel'][str(s)+':'+str(c)]['flagged'])
        totals.append(s_b['spw:channel'][str(s)+':'+str(c)]['total'])
        flag_frac.append(flagged[ct]/totals[ct])

fig=pylab.figure()
pylab.plot(freq,flag_frac,'k-')
pylab.xlim(940.,1445.)
pylab.xlabel('Frequency [MHz]')
pylab.ylabel('Fraction of data flagged')
pylab.savefig("bp_flag.png")
pylab.close(fig)

#16: plot delays
nplots=int(numAntenna/3)

if ((numAntenna%3)>0):
    nplots = nplots + 1

for ii in range(nplots):
    filename='finaldelay'+str(ii)+'.png'
    syscommand='rm -rf '+filename
    os.system(syscommand)
    
    antPlot=str(ii*3)+'~'+str(ii*3+2)
    
    default('plotcal')
    caltable='finaldelay.k'
    xaxis='freq'
    yaxis='delay'
    poln=''
    field=''
    antenna=antPlot
    spw=''
    timerange=''
    subplot=311
    overplot=False
    clearpanel='Auto'
    iteration='antenna'
    plotrange=[]
    showflags=False
    plotsymbol='o'
    plotcolor='blue'
    markersize=5.0
    fontsize=10.0
    showgui=False
    figfile=filename
    plotcal()

#17: Bandpass solutions
for ii in seq:
    default('plotms')
    vis='finalBPcal.b'
    field='1331+305=3C286'        # Only for 3C286
    xaxis='freq'
    yaxis='amp'
    xdatacolumn=''
    ydatacolumn=''
    averagedata=False
    spw=str(ii)
    iteraxis='spw'
    showlegend=False
    coloraxis='Antenna1'
    title='Spw'+str(ii)+': Amp v. Freq'
    showgui=False
    clearplots=True
    plotfile='bpamp.png'
    plotms()
    
    yaxis='phase'
    title='Spw'+str(ii)+': Phase v. Freq'
    plotfile='bpphase.png'
    plotms()

#Plot UV spectrum (averaged over all baselines & time) of flux calibrator
default('plotms')
vis=ms_active   
field='1331+305=3C286'        # Only for 3C286
xaxis='freq'
yaxis='amp'
xdatacolumn='corrected'
ydatacolumn='corrected'
averagedata=True
avgtime='1e5'
avgscan=True
avgbaseline=True
scalar=False
spw=''
avgspw=False
showlegend=False
coloraxis='spw'
plotrange=[0.95,1.43,0,0]
showgui=False
clearplots=True
plotfile='fluxcal_spectrum_full.png'
plotms()

plotrange=[0.95,1.43,14.5,18.5]
plotfile='fluxcal_spectrum_zoom.png'
plotms()


ms_name=ms_active[:-3]
output_ms=ms_name+'_flux_averaged.ms'

# Remove old averaged MS if it exists.
if os.path.exists(output_ms):
    os.system("rm -rf "+output_ms)
    
#18: average the data
default('oldsplit')
vis=ms_active
outputvis=output_ms
datacolumn='corrected'
field='1331+305=3C286'
spw='0~14:128~1920'    # Extend the region used for imaging/plots, only excluding the edges.
width='1793'
antenna=''
timebin=''
timerange=''
scan=''
intent=''
array=''
uvrange=''
correlation=''
observation=''
keepflags=False
keepmms=False
oldsplit()

# Use plotms to make plot of amplitude vs. phase, divided by spw
for ii in seq:
    default('plotms')
    vis=output_ms   # Take averaged 3C286 data for diagnostic plots
    field=''        # Only 3C286 in this MS.
    xaxis='phase'
    yaxis='amp'
    xdatacolumn='corrected'
    ydatacolumn='corrected'
    averagedata=False     
    spw=str(ii)
    avgspw=False
    showlegend=False
    iteraxis='spw'
    coloraxis='corr'
    showgui=False
    clearplots=True
    plotfile='fluxcal_ampphase.png'
    plotms()

# Use plotms to make plot of amplitude and phase vs. time, for each spw.  
for ii in seq:
    default('plotms')
    vis=output_ms  # File only contains 3C286, flux calibrator data
    field=''       
    xaxis='time'
    yaxis='amp'
    xdatacolumn='corrected'
    ydatacolumn='corrected'
    averagedata=False  # Data already averaged
    spw=str(ii)
    gridrows=1
    showlegend=False
    iteraxis='spw'
    coloraxis='corr'
    showgui=False
    clearplots=True
    plotfile='fluxcal_amptime.png'
    plotms()
    yaxis='phase'
    plotfile='fluxcal_phasetime.png'
    plotms()


#19: 
for ii in seq:
    #19: Imaging flux calibrator in continuum
    print 'STARTS IMAGING FLUX CALIBRATOR OF SPW='+str(ii)
    default('tclean')
    image_name='fluxcalibrator_spw'+str(ii)
    fieldid='1331*'
    grid_mode=''
    number_w=1
    image_size=[512,512]
    iteration=1000
    mask_name=['']
    
    vis=output_ms
    imagename=image_name
    selectdata=True
    datacolumn='data'
    field=fieldid
    spw=str(ii)
    specmode='mfs'
    nterms=1
    niter=iteration
    gain=0.1
    gridmode=grid_mode
    wprojplanes=number_w
    threshold='0.0mJy'
    deconvolver='clark'
    imagermode='csclean'
    cyclefactor=1.5
    cyclespeedup=-1
    multiscale=[]
    interactive=False
    mask=mask_name
    imsize=image_size
    cell=['1.5arcsec','1.5arcsec']
    stokes='I'
    weighting='briggs'
    robust=0.8
    uvtaper=[]
    modelimage=''
    restoringbeam=['']
    pblimit=-0.2
    pbcor=False
    usescratch=False
    allowchunk=False
    async=False
    parallel=True   
    tclean()

    
#20:  Calculate bmaj, bmin, peak, and rms for images of flux calibrator in each spw.
#     Output results as text file and plots of these values.
box_flux='300,30,460,200'

bmaj_flux=[]
bmin_flux=[]
max_flux=[]
rms_flux=[]

for ii in seq:
    
    image_fluxcal='fluxcalibrator_spw'+str(ii)+'.image'
    bmaj1=imhead(image_fluxcal,mode='get',hdkey='beammajor')
    if bmaj1==None:
        bmaj_flux.append(0.0)
        bmin_flux.append(0.0)
        max_flux.append(0.0)
        rms_flux.append(0.0)
    else:
        bmaj11=bmaj1['value']
        bmaj_flux.append(bmaj11)
        bmin1=imhead(image_fluxcal,mode='get',hdkey='beamminor')
        bmin11=bmin1['value']
        bmin_flux.append(bmin11)
        imst1=imstat(image_fluxcal)
        max1=imst1['max']
        max_flux.append(max1[0])
        imst1=imstat(image_fluxcal,box=box_flux)
        rms1=imst1['rms']
        rms_flux.append(rms1[0]*1e3)

# Output Image statistics in text file
f=open('statisticsFlux.txt','w')

print >> f, "Flux calibrator"
print >> f, "major axis [\"] \t", "".join(["%12.3f \t" %x for x in bmaj_flux])
print >> f, "minor axis [\"] \t", "".join(["%12.3f \t" %x for x in bmin_flux])
print >> f, "peak value [Jy] \t", "".join(["%12.3f \t" %x for x in max_flux])
print >> f, "RMS noise [mJy] \t", "".join(["%12.3f \t" %x for x in rms_flux])
f.close()

#Make plots of bmaj, bmin, peak, and rms
fig=pylab.figure()
pylab.plot(seq,bmaj_flux,'r--x',label='Bmaj')
pylab.plot(seq,bmin_flux,'b--x',label='Bmin')
pylab.xlabel('Spectral Window')
pylab.ylabel('Beam Size ["]')
pylab.legend()
pylab.savefig('fluxcal_beamsize.png')
pylab.close(fig)

fig=pylab.figure()
pylab.plot(seq,max_flux,'k-x')
pylab.xlabel('Spectral Window')
pylab.ylabel('Peak Flux [Jy]')
#pylab.yscale('log')
pylab.savefig('fluxcal_peak.png')
pylab.close(fig)

fig=pylab.figure()
pylab.plot(seq,rms_flux,'k-x')
pylab.xlabel('Spectral Window')
pylab.ylabel('RMS [mJy]')
pylab.yscale('log')
pylab.savefig('fluxcal_rms.png')
pylab.close(fig)

#Move plots, images to sub-directory

os.system("mv *.png plots")
if os.path.exists('images')==False:
    os.system("mkdir images")
os.system("mv fluxcalibrator_spw*.* images")

#Create webpage with BP results

if os.path.exists('bandpass.html'):
    os.system("rm bandpass.html")
wlog = open("bandpass.html","w")
wlog.write('<html>\n')
wlog.write('<head>\n')
wlog.write('<title>CHILES Pipeline Web Log</title>\n')
wlog.write('</head>\n')
wlog.write('<body>\n')
wlog.write('<br>\n')
wlog.write('<hr>\n')
wlog.write('<center>CHILES_pipe_bandpass results:</center>')
wlog.write('<li> Session: '+SDM_name+'</li>\n')
wlog.write('<li><a href="logs/bandpass.log">Bandpass Log</a></li>\n')
wlog.write('<li>Delay Solutions: \n')
wlog.write('<br><img src="plots/finaldelay0.png">\n')
wlog.write('<br><img src="plots/finaldelay1.png">\n')
wlog.write('<br><img src="plots/finaldelay2.png">\n')
wlog.write('<br><img src="plots/finaldelay3.png">\n')
wlog.write('<br><img src="plots/finaldelay4.png">\n')
wlog.write('<br><img src="plots/finaldelay5.png">\n')
wlog.write('<br><img src="plots/finaldelay6.png">\n')
wlog.write('<br><img src="plots/finaldelay7.png">\n')
wlog.write('<br><img src="plots/finaldelay8.png"></li>\n')
wlog.write('<li> Bandpass solutions (amplitude and phase) for reference antenna: \n')
wlog.write('<li> Color coded by antenna, both polarizations shown \n')
wlog.write('<table> \n')
for ii in seq:
    wlog.write('<tr><td><img src="plots/bpamp_Spw'+str(ii)+'.png"></td>\n')
    wlog.write('<td><img src="plots/bpphase_Spw'+str(ii)+'.png"></td></tr>\n')
wlog.write('</table> \n')
wlog.write('<br>')
wlog.write('<li> Amp vs. Phase (averaged over all channels in a spw): \n')
for ii in seq:
    wlog.write('<br><img src="plots/fluxcal_ampphase_Spw'+str(ii)+'.png">\n')
wlog.write('<li> Spectrum of Flux calibrator (both LL & RR, averaged over all time & baselines): \n')
wlog.write('<br><img src="plots/fluxcal_spectrum_full.png">\n')
wlog.write('<br><img src="plots/fluxcal_spectrum_zoom.png">\n')
wlog.write('<li> Amp. & Phase vs. time for Flux Calibrator (averaged over frequency): \n')
wlog.write('<table> \n')
for ii in seq:
    wlog.write('<tr><td><img src="plots/fluxcal_amptime_Spw'+str(ii)+'.png"></td>\n')
    wlog.write('<td><img src="plots/fluxcal_phasetime_Spw'+str(ii)+'.png"></td></tr>\n')
wlog.write('</table> \n')
wlog.write('</li>')
wlog.write('<li> Measured properties of flux calibrator: \n')
wlog.write('<br><img src="plots/fluxcal_beamsize.png">\n')
wlog.write('<br><img src="plots/fluxcal_peak.png">\n')
wlog.write('<br><img src="plots/fluxcal_rms.png"></li>\n')
wlog.write('<br>\n')
wlog.write('<br> Flagging percentage vs. frequency:\n')
wlog.write('<li><br><img src="./plots/bp_flag.png"></li>\n')
wlog.write('<br>\n')
wlog.write('<br> Flagging percentage vs. uvdist\n')
wlog.write('<li><br><img src="./plots/bp_flag_uvdist.png"></li>\n')
wlog.write('<br>\n')
wlog.write('<br> Percentage of 3C286 data flagged: '+str(flux_flag*100)+'\n')
wlog.write('<br>')
wlog.write('<hr>\n')
wlog.write('</body>\n')
wlog.write('</html>\n')
wlog.close()

# Make QA plots for phasecal module
# Step 10: Makes diagnostic plots for assessment
# Look at calibration tables for phase cal (amp, phase vs. time, frequency)
# Make images of phase cal and look at flux,beam vs. spw

logprint ("Making diagnostic plots for phase calibrator", logfileout='logs/QA.log')

# Make plot of flagging statistics
# s_p is the output of flagdata run (above)
# Get information for flagging percentage vs. uvdistance
#gantdata = get_antenna_data(ms_active)
#create adictionary with flagging info
#base_dict = create_baseline_dict(ms_active, gantdata)
#gantdata and base_dict are already in the initial module so no need to retrieve that information again.
#match flagging data to dictionary entry
datamatch = flag_match_baseline(s_p['baseline'], base_dict)
#bin the statistics
binned_stats = bin_statistics(datamatch, 'B', 25)  # 25 is the number of uvdist bins such that there is minimal error in uvdist.

#Plot flagging % vs. uvdist
### Plot the Data
barwidth = binned_stats[0][1]
totflagged = 'Phase Cal Flagging: '+ str(phase_flag*100) + '% Data Flagged'
pylab.close()
pylab.bar(binned_stats[0],binned_stats[1], width=barwidth, color='grey', align='edge')
pylab.title(totflagged)
pylab.grid()
pylab.ylabel('flagged data [%]')
pylab.xlabel('average UV distance [m]')
pylab.savefig('phase_flag_uvdist.png')
pylab.close()

os.system("mv phase_flag_uvdist.png plots/.") 

# Make plot of percentage of data flagged as a function of channel (for both correlations combined):
flag_frac=[]
ct=-1
chan=[]
freq=[]
flagged=[]
totals=[]
# Extract frequency of first channel of spw=0 from listobs output
nu0=reference_frequencies[0]/1.e6 #get reference frequency in MHz
dnu=0.015625 # channel width in MHz
for s in range(15):
    for c in range(2048):
        ct+=1
        chan.append(ct)
        freq.append(nu0+dnu*ct)
        flagged.append(s_p['spw:channel'][str(s)+':'+str(c)]['flagged'])
        totals.append(s_p['spw:channel'][str(s)+':'+str(c)]['total'])
        flag_frac.append(flagged[ct]/totals[ct])

fig=pylab.figure()
pylab.plot(freq,flag_frac,'k-')
pylab.xlim(940.,1445.)
pylab.xlabel('Frequency [MHz]')
pylab.ylabel('Fraction of data flagged')
pylab.savefig("phase_flag.png")
pylab.close(fig)

#Plot UV spectrum (averaged over all baselines & time) of phase calibrator
default('plotms')
vis=ms_active   
field='J0943-0819'        # Only for J0943
xaxis='freq'
yaxis='amp'
xdatacolumn='corrected'
ydatacolumn='corrected'
averagedata=True
avgtime='1e5'
avgscan=True
avgbaseline=True
scalar=False
spw=''
avgspw=False
showlegend=False
coloraxis='spw'
plotrange=[0.95,1.43,0,0]
showgui=False
clearplots=True
plotfile='phasecal_spectrum_full.png'
plotms()

plotrange=[0.95,1.43,2.7,3.5]
plotfile='phasecal_spectrum_zoom.png'
plotms()


ms_name=ms_active[:-3]
output_ms=ms_name+'_phasecal_averaged.ms'

# Remove old averaged MS if it exists.
if os.path.exists(output_ms):
    os.system("rm -rf "+output_ms)
    
#Split the phase cal.
default('oldsplit')
vis=ms_active
outputvis=output_ms
datacolumn='corrected'
field='J0943-0819'
spw='0~14:128~1920'  # Average over all channels, except the very edges
width='1793'
antenna=''
timebin=''
timerange=''
scan=''
intent=''
array=''
uvrange=''
correlation=''
observation=''
keepflags=False
keepmms=False
oldsplit()

# Use plotms to make plot of amplitude vs. phase, divided by spw

for ii in seq:
    default('plotms')
    vis=output_ms  # File only contains J0943, phase calibrator data
    field=''       
    xaxis='phase'
    yaxis='amp'
    xdatacolumn='corrected'
    ydatacolumn='corrected'
    averagedata=False  # Data already averaged
    spw=str(ii)
    gridrows=1
    showlegend=False
    iteraxis='spw'
    coloraxis='corr'
    showgui=False
    clearplots=True
    plotfile='phasecal_ampphase.png'
    plotms()

# Use plotms to make plot of amplitude and phase vs. time, for each spw.  
for ii in seq:
    default('plotms')
    vis=output_ms  # File only contains J0943, phase calibrator data
    field=''       
    xaxis='time'
    yaxis='amp'
    xdatacolumn='corrected'
    ydatacolumn='corrected'
    averagedata=False  # Data already averaged
    spw=str(ii)
    gridrows=1
    showlegend=False
    iteraxis='spw'
    coloraxis='corr'
    showgui=False
    clearplots=True
    plotfile='phasecal_amptime.png'
    plotms()
    yaxis='phase'
    plotfile='phasecal_phasetime.png'
    plotms()

#Image phase cal: 
for ii in seq:
    print 'STARTS IMAGING PHASE CALIBRATOR OF SPW='+str(ii)
    default('tclean')
    image_name='phasecalibrator_spw'+str(ii)
    fieldid='J0943*'
    grid_mode=''
    number_w=1
    image_size=[512,512]
    iteration=1000
    mask_name=['']
    
    vis=output_ms
    imagename=image_name
    selectdata=True
    datacolumn='data'
    field=fieldid
    spw=str(ii)
    specmode='mfs'
    nterms=1
    niter=iteration
    gain=0.1
    gridmode=grid_mode
    wprojplanes=number_w
    threshold='0.0mJy'
    deconvolver='clark'
    imagermode='csclean'
    cyclefactor=1.5
    cyclespeedup=-1
    multiscale=[]
    interactive=False
    mask=mask_name
    imsize=image_size
    cell=['1.5arcsec','1.5arcsec']
    stokes='I'
    weighting='briggs'
    robust=0.8
    uvtaper=[]
    modelimage=''
    restoringbeam=['']
    pblimit=-0.2
    pbcor=False
    usescratch=False
    allowchunk=False
    async=False
    parallel=True
    tclean()
    

#     Calculate bmaj, bmin, peak, and rms for images of flux calibrator in each spw.
#     Output results as text file and plots of these values.
box_phase='300,30,460,200'

bmaj_phase=[]
bmin_phase=[]
max_phase=[]
rms_phase=[]

for ii in seq:
    
    image_phasecal='phasecalibrator_spw'+str(ii)+'.image'
    bmaj1=imhead(image_phasecal,mode='get',hdkey='beammajor')
    if bmaj1==None:
        bmaj_phase.append(0.0)
        bmin_phase.append(0.0)
        max_phase.append(0.0)
        rms_phase.append(0.0)
    else:
        bmaj11=bmaj1['value']
        bmaj_phase.append(bmaj11)
        bmin1=imhead(image_phasecal,mode='get',hdkey='beamminor')
        bmin11=bmin1['value']
        bmin_phase.append(bmin11)
        imst1=imstat(image_phasecal)
        max1=imst1['max']
        max_phase.append(max1[0])
        imst1=imstat(image_phasecal,box=box_phase)
        rms1=imst1['rms']
        rms_phase.append(rms1[0]*1e3)

# Output Image statistics in text file
f=open('statisticsPhase.txt','w')

print >> f, "Phase calibrator"
print >> f, "major axis [\"] \t", "".join(["%12.3f \t" %x for x in bmaj_phase])
print >> f, "minor axis [\"] \t", "".join(["%12.3f \t" %x for x in bmin_phase])
print >> f, "peak value [Jy] \t", "".join(["%12.3f \t" %x for x in max_phase])
print >> f, "RMS noise [mJy] \t", "".join(["%12.3f \t" %x for x in rms_phase])
f.close()

#Make plots of bmaj, bmin, peak, and rms
fig=pylab.figure()
pylab.plot(seq,bmaj_phase,'r--x',label='Bmaj')
pylab.plot(seq,bmin_phase,'b--x',label='Bmin')
pylab.xlabel('Spectral Window')
pylab.ylabel('Beam Size ["]')
pylab.legend()
pylab.savefig('phasecal_beamsize.png')
pylab.close(fig)

fig=pylab.figure()
pylab.plot(seq,max_phase,'k-x')
pylab.xlabel('Spectral Window')
pylab.ylabel('Peak Flux [Jy]')
#pylab.yscale('log')
pylab.savefig('phasecal_peak.png')
pylab.close(fig)

fig=pylab.figure()
pylab.plot(seq,rms_phase,'k-x')
pylab.xlabel('Spectral Window')
pylab.ylabel('RMS [mJy]')
pylab.yscale('log')
pylab.savefig('phasecal_rms.png')
pylab.close(fig)

#Plot calibration tables
default('plotcal')
caltable='finalamp.gcal'
xaxis='time'
yaxis='amp'
showgui=False
figfile='caltable_finalamp_amp.png'
plotcal()

yaxis='phase'
figfile='caltable_finalamp_phase.png'
plotcal()

#Move plots, images to sub-directory

os.system("mv *.png plots")
if os.path.exists('images')==False:
    os.system("mkdir images")
os.system("mv phasecalibrator_spw*.* images")

#Create webpage with phasecal results

if os.path.exists('phasecal.html'):
    os.system("rm phasecal.html")
wlog = open("phasecal.html","w")
wlog.write('<html>\n')
wlog.write('<head>\n')
wlog.write('<title>CHILES Pipeline Web Log</title>\n')
wlog.write('</head>\n')
wlog.write('<body>\n')
wlog.write('<br>\n')
wlog.write('<hr>\n')
wlog.write('<center>CHILES_pipe_phasecal results:</center>')
wlog.write('<li> Session: '+SDM_name+'</li>\n')
wlog.write('<li><a href="logs/phasecal.log">Phasecal Log</a></li>\n')
wlog.write('<li>finalamp.gcal Amp vs. Time: \n')
wlog.write('<br><img src="plots/caltable_finalamp_amp.png"></li>\n')
wlog.write('<li>finalamp.gcal Phase vs. Time: \n')
wlog.write('<br><img src="plots/caltable_finalamp_phase.png"></li>\n')
wlog.write('<li> Amp vs. Phase: \n')
for ii in seq:
    wlog.write('<br><img src="plots/phasecal_ampphase_Spw'+str(ii)+'.png">\n')
wlog.write('<li> Spectrum of Phase calibrator (both LL & RR, averaged over all time & baselines): \n')
wlog.write('<br><img src="plots/phasecal_spectrum_full.png">\n')
wlog.write('<br><img src="plots/phasecal_spectrum_zoom.png">\n')
wlog.write('<li> Amp. & Phase vs. time for Phase Calibrator (averaged over frequency): \n')
wlog.write('<table> \n')
for ii in seq:
    wlog.write('<tr>\n')
    wlog.write('<td><img src="plots/phasecal_amptime_Spw'+str(ii)+'.png"></td>\n')
    wlog.write('<td><img src="plots/phasecal_phasetime_Spw'+str(ii)+'.png"></td>\n')
    wlog.write('</tr>\n')
wlog.write('</table> \n')
wlog.write('</li>')
wlog.write('<li> Measured properties of phase calibrator: \n')
wlog.write('<br><img src="plots/phasecal_beamsize.png">\n')
wlog.write('<br><img src="plots/phasecal_peak.png">\n')
wlog.write('<br><img src="plots/phasecal_rms.png"></li>\n')
wlog.write('<br>\n')
wlog.write('<br> Flagging percentage vs. frequency:\n')
wlog.write('<li><br><img src="./plots/phase_flag.png"></li>\n')
wlog.write('<br>\n')
wlog.write('<br> Flagging percentage vs. uvdist\n')
wlog.write('<li><br><img src="./plots/phase_flag_uvdist.png"></li>\n')
wlog.write('<br>\n')
wlog.write('<br> Percentage of J0943 data flagged: '+str(phase_flag*100)+'\n')
wlog.write('<br>')
wlog.write('<hr>\n')
wlog.write('</body>\n')
wlog.write('</html>\n')
wlog.close()

# Make QA plots for target module

# Step 3: Diagnostic Plots
logprint("Making diagnostic plots",logfileout='logs/QA.log')

# Make plot of percentage of data flagged as a function of channel (for both correlations combined):
flag_frac=[]
ct=-1
chan=[]
freq=[]
flagged=[]
totals=[]
# Extract frequency of first channel of spw=0 from listobs output
nu0=reference_frequencies[0]/1.e6 #get reference frequency in MHz
dnu=0.015625 # channel width in MHz
freq=[]

for s in range(15):
    for c in range(2048):
        ct+=1
        chan.append(ct)
        freq.append(nu0+dnu*ct)
        flagged.append(s_t['spw:channel'][str(s)+':'+str(c)]['flagged'])
        totals.append(s_t['spw:channel'][str(s)+':'+str(c)]['total'])
        flag_frac.append(flagged[ct]/totals[ct])

fig=pylab.figure()
pylab.plot(freq,flag_frac,'k-')
pylab.xlim(940.,1445.)
pylab.xlabel('Frequency [MHz]')
pylab.ylabel('Fraction of data flagged')
pylab.savefig("target_flag.png")
pylab.close(fig)

#Plot UV spectrum (averaged over all baselines & time) of target
default('plotms')
vis=ms_active   
field='deepfield'        # Only for deepfield
xaxis='freq'
yaxis='amp'
xdatacolumn='corrected'
ydatacolumn='corrected'
averagedata=True
avgtime='1e5'
avgscan=True
avgbaseline=True
scalar=False
spw=''
avgspw=False
showlegend=False
coloraxis='spw'
plotrange=[0.95,1.43,0,0]
showgui=False
clearplots=True
plotfile='target_spectrum_full.png'
plotms()

plotrange=[0.95,1.43,-0.0001,0.015]
plotfile='target_spectrum_zoom.png'
plotms()

# Use plotms to make plot of amplitude vs. frequency, divided by spw

for ii in seq:
    default('plotms')
    vis=ms_active  # Use standard MS
    field='deepfield'       # Only for deepfield
    xaxis='frequency'
    yaxis='amp'
    xdatacolumn='corrected'
    ydatacolumn='corrected'
    averagedata=True
    avgtime='1e5'
    avgscan=True   
    spw=str(ii)
    gridrows=1
    showlegend=False
    iteraxis='spw'
    coloraxis='corr'
    showgui=False
    clearplots=True
    plotfile='target_ampchannel.png'
    plotms()
    
# Make amplitude vs. time plot
    xaxis='time'
    avgtime=''
    avgscan=False
    avgchannel='2048'
    plotfile='target_amptime.png'
    plotms()

#Split the target.
ms_name=ms_active[:-3]
output_ms=ms_name+'_target_averaged.ms'

default('oldsplit')
vis=ms_active
outputvis=output_ms
datacolumn='corrected'
field='deepfield'
spw='0~14:128~1920'  # Extend averaging to include all but 128 edge channels
width='1793'
antenna=''
timebin=''
timerange=''
scan=''
intent=''
array=''
uvrange=''
correlation=''
observation=''
keepflags=False
keepmms=False
oldsplit()


#Image target: 
for ii in seq:
    print 'STARTS IMAGING Deepfield OF SPW='+str(ii)
    default('tclean')
    image_name='target_spw'+str(ii)
    fieldid='deepfield'
    grid_mode=''
    number_w=1
    image_size=[2048,2048]
    iteration=1000
    mask_name=['']
    vis=output_ms
    imagename=image_name
    selectdata=True
    datacolumn='data'
    field=fieldid
    spw=str(ii)
    specmode='mfs'
    nterms=1
    niter=iteration
    gain=0.1
    gridmode=grid_mode
    wprojplanes=number_w
    threshold='0.0mJy'
    deconvolver='clark'
    imagermode='csclean'
    cyclefactor=1.5
    cyclespeedup=-1
    multiscale=[]
    interactive=False
    mask=mask_name
    imsize=image_size
    cell=['1.5arcsec','1.5arcsec']
    stokes='I'
    weighting='briggs'
    robust=0.8
    uvtaper=[]
    modelimage=''
    restoringbeam=['']
    pblimit=-0.2
    pbcor=False
    usescratch=False
    allowchunk=False
    async=False
    parallel=True
    tclean()

# Measure statistics of deepfield image:
box_target='1300,1100,1900,1600'
bmaj_target=[]
bmin_target=[]
max_target=[]
rms_target=[]


logprint("START READING STATISTICS FROM IMHEAD() AND IMSTAT()",logfileout='logs/QA.log')
for ii in seq:
	image_target='target_spw'+str(ii)+'.image'
	bmaj1=imhead(image_target,mode='get',hdkey='beammajor')
	if bmaj1==None :
		bmaj_target.append(0.0)
		bmin_target.append(0.0)
		max_target.append(0.0)
		rms_target.append(0.0)
	else:
		bmaj11=bmaj1['value']
		bmaj_target.append(bmaj11)
		bmin1=imhead(image_target,mode='get',hdkey='beamminor')
		bmin11=bmin1['value']
		bmin_target.append(bmin11)
		imst1=imstat(image_target)
		max1=imst1['max']
		max_target.append(max1[0]*1e3)
		imst1=imstat(image_target,box=box_target)
		rms1=imst1['rms']
		rms_target.append(rms1[0]*1e6)

logprint("FINISH READING STATISTICS FROM IMHEAD() AND IMSTAT()",logfileout='logs/QA.log')

f=open('statisticsTarget.txt','w')
print >> f, "Deep field"
print >> f, "major axis [\"] \t", "".join(["%12.3f \t" %x for x in bmaj_target])
print >> f, "minor axis [\"] \t", "".join(["%12.3f \t" %x for x in bmin_target])
print >> f, "peak value [mJy] \t", "".join(["%12.3f \t" %x for x in max_target])
print >> f, "RMS noise [uJy] \t", "".join(["%12.3f \t" %x for x in rms_target])

f.close()

#Make plots of bmaj, bmin, peak, and rms
fig=pylab.figure()
pylab.plot(seq,bmaj_target,'r--x',label='Bmaj')
pylab.plot(seq,bmin_target,'b--x',label='Bmin')
pylab.xlabel('Spectral Window')
pylab.ylabel('Beam Size ["]')
pylab.legend()
pylab.savefig('target_beamsize.png')
pylab.close(fig)

fig=pylab.figure()
pylab.plot(seq,max_target,'k-x')
pylab.xlabel('Spectral Window')
pylab.ylabel('Peak Flux [mJy]')
#pylab.yscale('log')
pylab.savefig('target_peak.png')
pylab.close(fig)

fig=pylab.figure()
pylab.plot(seq,rms_target,'k-x')
pylab.xlabel('Spectral Window')
pylab.ylabel('RMS [uJy]')
pylab.yscale('log')
pylab.savefig('target_rms.png')
pylab.close(fig)

#Want to plot image of flux calibrator in each spw.  Use "imview"

for ii in seq:
    image_target='target_spw'+str(ii)+'.image'
    kntr_levels=[-2*rms_target[ii]/1000.,2*rms_target[ii]/1000.,0.1*max_target[ii],0.3*max_target[ii],0.5*max_target[ii],0.7*max_target[ii],0.9*max_target[ii]]
    imview(raster={'file':image_target, 'colorwedge':True},contour={'file':image_target,'levels':kntr_levels},out='target_spw'+str(ii)+'.png')


#Move plots, images to sub-directory

os.system("mv *.png plots")
os.system("mv target_spw*.* images")

#Create webpage with results

if os.path.exists("target.html"):
    os.system("rm target.html")
wlog = open("target.html","w")
wlog.write('<html>\n')
wlog.write('<head>\n')
wlog.write('<title>CHILES Pipeline Web Log</title>\n')
wlog.write('</head>\n')
wlog.write('<body>\n')
wlog.write('<br>\n')
wlog.write('<hr>\n')
wlog.write('<center>CHILES_pipe_target results:</center>')
wlog.write('<li> Session: '+SDM_name+'</li>\n')
wlog.write('<li><a href="logs/target.log">Target Log</a></li>\n')
wlog.write('<li> Amp vs. Frequency (time-averaged, all channels shown) and Amp vs. Time (all channels averaged): \n')
wlog.write('<table> \n')
for ii in seq:
    wlog.write('<tr>\n')
    wlog.write('<td><img src="plots/target_ampchannel_Spw'+str(ii)+'.png"></td>\n')
    wlog.write('<td><img src="plots/target_amptime_Spw'+str(ii)+'.png"></td>\n')
    wlog.write('</tr>\n')
wlog.write('</table> \n')
wlog.write('<li> Spectrum of Deepfield (both LL & RR, averaged over all time & baselines): \n')
wlog.write('<br><img src="plots/target_spectrum_full.png">\n')
wlog.write('<br><img src="plots/target_spectrum_zoom.png">\n')
wlog.write('<li> Images of Deepfield: \n')
for ii in seq:
    wlog.write('<br><img src="plots/target_spw'+str(ii)+'.png">\n')
wlog.write('</li>')
wlog.write('<li> Measured properties of deepfield: \n')
wlog.write('<br><img src="plots/target_beamsize.png">\n')
wlog.write('<br><img src="plots/target_peak.png">\n')
wlog.write('<br><img src="plots/target_rms.png"></li>\n')
wlog.write('<br>\n')
wlog.write('<br> Percentage of deepfield data flagged: '+str(target_flag*100)+'\n')
wlog.write('<br> Flagging percentage vs. frequency (before removing channels that are more than 90% flagged)\n')
wlog.write('<br><img src="plots/target_flag.png">\n')
wlog.write('<br>')
wlog.write('<hr>\n')
wlog.write('</body>\n')
wlog.write('</html>\n')
wlog.close()


# Make cubes for QA purposes
# Split data with no averaging (will slow down imaging, but avoids other issues)
# Skipping imaging cube of flux calibrator 

logprint('Making QA cubes of flux and phase calibrator plus target field', logfileout='logs/QA.log')

#fluxms=ms_active[:-3]+'_calibrated_fluxcal'
phasems=ms_active[:-3]+'_calibrated_phasecal'
targetms=ms_active[:-3]+'_calibrated_deepfield'



for ii in seq:

    logprint ('Split calibrated uv data, Spw='+str(ii), logfileout='logs/QA.log')

#    fluxfile=fluxms+'_spw'+str(ii)+'.ms'
    phasefile=phasems+'_spw'+str(ii)+'.ms'
    targetfile=targetms+'_spw'+str(ii)+'.ms'
    # if os.path.exists(fluxfile):
    #     os.system("rm -rf "+fluxfile)
    #     os.system("rm -rf fluxcube_spw"+str(ii)+".*")
    #     os.system("rm -rf images/fluxcube_spw"+str(ii)+".*")
    if os.path.exists(phasefile):
        os.system("rm -rf "+phasefile)
        os.system("rm -rf phasecube_spw"+str(ii)+".*")
        os.system("rm -rf images/phasecube_spw"+str(ii)+".*")
    if os.path.exists(targetfile):
        os.system("rm -rf "+targetfile)
        os.system("rm -rf targetcube_10mJy_spw"+str(ii)+".*")
        os.system("rm -rf targetcube_HIdet_spw"+str(ii)+".*")
        os.system("rm -rf images/targetcube_10mJy_spw"+str(ii)+".*")
        os.system("rm -rf images/targetcube_HIdet_spw"+str(ii)+".*")

    try:

# # Split off flux calibrator
#         default('oldsplit')
#         vis=ms_active
#         datacolumn='corrected'
#         outputvis=fluxfile
#         field='1331+305=3C286'
#         spw =str(ii)
#         width=1
#         timebin=''
#         oldsplit()
  
# Split off phase calibrator
        default('oldsplit')
        vis=ms_active
        datacolumn='corrected'
        outputvis=phasefile
        field='J0943-0819'
        spw =str(ii)
        width=1
        timebin=''
        oldsplit()
                
# Split off deepfield                
        default('oldsplit')
        vis=ms_active
        datacolumn='corrected'
        outputvis=targetfile
        field='deepfield'
        spw =str(ii)
        width=1
        timebin=''
        oldsplit()
    except:
        logprint ('Unable to split uv data, Spw='+str(ii), logfileout='logs/QA.log')


# Now make cubes
for ii in seq:
    logprint ('Image calibrated uv data, Spw='+str(ii), logfileout='logs/QA.log')

    try:

        default('tclean')
# 
#         vis=fluxfile
#         imagename=image_name
#         selectdata=False
#         field=fieldid
#         spw=''
#         specmode='cubedata'
#         nterms=1
#         niter=iteration
#         gain=0.1
#         gridmode=grid_mode
#         wprojplanes=number_w
#         threshold='10.0mJy'  # Adding threshold
#         deconvolver='clark'
#         imagermode='csclean'
#         cyclefactor=1.5
#         cyclespeedup=-1
#         multiscale=[]
#         interactive=False
#         mask=mask_name
#         imsize=image_size
#         cell=['1.5arcsec','1.5arcsec']
#         stokes='I'
#         weighting='briggs'
#         robust=0.8
#         uvtaper=[]
#         modelimage=''
#         restoringbeam=['']
#         pblimit=-0.2
#         pbcor=False
#         usescratch=False
#         allowchunk=False
#         async=False
#         tclean()

# Make test cube of phase calibrator, all channels
        logprint ('Make cube of J0943-0819, all channels, Spw='+str(ii), logfileout='logs/QA.log')

        default('tclean')
    
        phasefile=phasems+'_spw'+str(ii)+'.ms'
        image_name='phasecube_spw'+str(ii)
        fieldid='J0943-0819'
        grid_mode=''
        number_w=1
        mask_name=['']    
        image_size=[64,64]
        iteration=2048   # Total number of iterations for the entire cube (per spw), ~1 per channel 


        vis=phasefile
        imagename=image_name
        selectdata=False
        field=fieldid
        spw=''
        specmode='cubedata'
        nterms=1
        niter=iteration
        gain=0.1
        gridmode=grid_mode
        wprojplanes=number_w
        threshold='10.0mJy'  # Adding threshold
        deconvolver='clark'
        imagermode='csclean'
        cyclefactor=1.5
        cyclespeedup=-1
        multiscale=[]
        interactive=False
        mask=mask_name
        imsize=image_size
        cell=['1.5arcsec','1.5arcsec']
        stokes='I'
        weighting='briggs'
        robust=0.8
        uvtaper=[]
        modelimage=''
        restoringbeam=['']
        pblimit=-0.2
        pbcor=False
        usescratch=False
        allowchunk=False
        async=False
        parallel=True
        tclean()


# Make test cube of 10 mJy source, all channels
        logprint ('Make cube of 10 mJy source, all channels, Spw='+str(ii), logfileout='logs/QA.log')

        default('tclean')
    
        targetfile=targetms+'_spw'+str(ii)+'.ms'
        image_name='targetcube_10mJy_spw'+str(ii)
        fieldid='deepfield'
        phasecenter='J2000 10h01m31.4 +02d26m40'
        iteration=2048   # Total number of iterations for the entire cube (per spw), ~1 per channel 

        vis=targetfile
        imagename=image_name
        selectdata=False
        field=fieldid
        spw=''
        specmode='cubedata'
        nterms=1
        niter=iteration
        gain=0.1
        gridmode=grid_mode
        wprojplanes=number_w
        threshold='10.0mJy'  # Adding threshold
        deconvolver='clark'
        imagermode='csclean'
        cyclefactor=1.5
        cyclespeedup=-1
        multiscale=[]
        interactive=False
        mask=mask_name
        imsize=image_size
        cell=['1.5arcsec','1.5arcsec']
        stokes='I'
        weighting='briggs'
        robust=0.8
        uvtaper=[]
        modelimage=''
        restoringbeam=['']
        pblimit=-0.2
        pbcor=False
        usescratch=False
        allowchunk=False
        async=False
        parallel=True
        tclean()

# Make small cube of lowest z detection
        logprint ('Make cube of strongest pilot detection, Spw='+str(ii), logfileout='logs/QA.log')

        default('tclean')
    
        image_name='targetcube_HIdet_spw'+str(ii)
        fieldid='deepfield'
        phasecenter='J2000 10h01m15.2 +02d18m24'
        iteration=2048   # Total number of iterations for the entire spw cube, ~1 per channel, only used for HI source spw 
        
        vis=targetfile
        imagename=image_name
        selectdata=False
        field=fieldid
        spw=''
        specmode='cubedata'
        nterms=1
        niter=iteration
        gain=0.1
        gridmode=grid_mode
        wprojplanes=number_w
        threshold='10.0mJy'
        deconvolver='clark'
        imagermode='csclean'
        cyclefactor=1.5
        cyclespeedup=-1
        multiscale=[]
        interactive=False
        mask=mask_name
        imsize=image_size
        cell=['1.5arcsec','1.5arcsec']
        stokes='I'
        weighting='briggs'
        robust=0.8
        uvtaper=[]
        modelimage=''
        restoringbeam=['']
        pblimit=-0.2
        pbcor=False
        usescratch=False
        allowchunk=False
        async=False
        parallel=True
        if ii==13:
            tclean()
          
    except:
        logprint("Unable to image "+str(field)+", spw "+str(ii), logfileout='logs/QA.log')

logprint("Finished making all cubes", logfileout='logs/QA.log')


# Prepare to extract information about cubes.
# Initialize variables

sigma=np.zeros(15,float)

# bmaj
bmaj_f=[]
bmaj_p=[]
bmaj_t10=[]
bmaj_tHI=[]

# bmin
bmin_f=[]
bmin_p=[]
bmin_t10=[]
bmin_tHI=[]

# rms
fsigma=[]
psigma=[]
t10sigma=[]
tHIsigma=[]

# flux
bp_flux=[]
p_flux=[]
t10_flux=[]
tHI_flux=[]

# Frequencies
freq_f=[]
freq_p=[]
freq_t10=[]
freq_tHI=[]


for ii in seq:       
    logprint ('Extract Data from Cubes, Spw='+str(ii), logfileout='logs/QA.log')
    
#Extract Flux calibrator data
#     logprint ('Extract Flux Calibrator data, Spw='+str(ii), logfileout='logs/QA.log')
# 
#     try:
#         image_name='fluxcube_spw'+str(ii)+'.image'
#         header=imhead(image_name)
#         default('imval')
#         stokes='I'
#         imagename=image_name
#         results=imval()
#         for i in range(2048):
#             bmaj_f.append(header['perplanebeams']['beams']['*'+str(i)]['*0']['major']['value'])
#             bmin_f.append(header['perplanebeams']['beams']['*'+str(i)]['*0']['minor']['value'])
#             freq_f.append(results['coords'][i,3]/1e6)
#             bp_flux.append(results['data'][i])
#                     
#         default('imval')
#         stokes='I'
#         box='0,0,16,16'
#         imagename=image_name
#         results=imval()
#         for i in range(2048):
#             fsigma.append(np.std(results['data'][:,:,i]))
#         
#     except:
#         logprint("Unable to extract data from flux calibrator cube, spw "+str(ii), logfileout='logs/QA.log')
        
  
#Extract Phase calibrator data
    logprint ('Extract Phase Calibrator data, Spw='+str(ii), logfileout='logs/QA.log')
    try:
        image_name='phasecube_spw'+str(ii)+'.image'
        header=imhead(image_name)
        default('imval')
        stokes='I'
        imagename=image_name
        results=imval()
        for i in range(2048):
            bmaj_p.append(header['perplanebeams']['beams']['*'+str(i)]['*0']['major']['value'])
            bmin_p.append(header['perplanebeams']['beams']['*'+str(i)]['*0']['minor']['value'])
            freq_p.append(results['coords'][i,3]/1e6)
            p_flux.append(results['data'][i])
                    
        default('imval')
        stokes='I'
        box='0,0,16,16'
        imagename=image_name
        results=imval()
        for i in range(2048):
            psigma.append(np.std(results['data'][:,:,i]))
    except:
        logprint("Unable to extract data from phase calibrator cube, spw "+str(ii), logfileout='logs/QA.log')

#Extract spectra for 10 mJy cube
    try:
        logprint ('Extract Spectra from 10 mJy cube, Spw='+str(ii), logfileout='logs/QA.log')
        image_name='targetcube_10mJy_spw'+str(ii)+'.image'
        header=imhead(image_name)
        default('imval')
        stokes='I'
        imagename=image_name
        results=imval()
        for i in range(2048):
            bmaj_t10.append(header['perplanebeams']['beams']['*'+str(i)]['*0']['major']['value'])
            bmin_t10.append(header['perplanebeams']['beams']['*'+str(i)]['*0']['minor']['value'])
            freq_t10.append(results['coords'][i,3]/1e6)
            t10_flux.append(results['data'][i])
            
        default('imval')
        stokes='I'
        box='0,0,16,16'
        imagename=image_name
        results=imval()
        for i in range(2048):
            t10sigma.append(np.std(results['data'][:,:,i]))

        sigma[ii]=np.std(results['data'][:,:,:])  # Take noise averaged over all channels of 10 mJy cube
              
    except:
        logprint("Unable to extract data from 10 mJy cube, spw "+str(ii), logfileout='logs/QA.log')

#Extract spectrum for deepfield cubes
    if ii==13:
        try:    
            logprint ('Extract Spectra from HI source cube, Spw='+str(ii), logfileout='logs/QA.log')    
            image_name='targetcube_HIdet_spw'+str(ii)+'.image'
            header=imhead(image_name)
            default('imval')
            stokes='I'
            box='16,16,48,48'
            imagename=image_name
            results=imval()
            for i in range(2048):
                bmaj_tHI.append(header['perplanebeams']['beams']['*'+str(i)]['*0']['major']['value'])
                bmin_tHI.append(header['perplanebeams']['beams']['*'+str(i)]['*0']['minor']['value'])
                freq_tHI.append(results['coords'][0,0,i,3]/1e6)
                tHI_flux.append(np.mean(results['data'][:,:,i]))
                
            default('imval')
            stokes='I'
            box='0,0,16,16'
            imagename=image_name
            results=imval()
            for i in range(2048):
                tHIsigma.append(np.std(results['data'][:,:,i]))
                

    
        except:
            logprint("Unable to extract data from HI source cube, spw "+str(ii), logfileout='logs/QA.log')

# Convert lists to arrays and replace zeros with nan (for plotting)
bmaj_f=np.array(bmaj_f,float)
bmaj_p=np.array(bmaj_p,float)
bmaj_t10=np.array(bmaj_t10,float)
bmaj_tHI=np.array(bmaj_tHI,float)
bmin_f=np.array(bmin_f,float)
bmin_p=np.array(bmin_p,float)
bmin_t10=np.array(bmin_t10,float)
bmin_tHI=np.array(bmin_tHI,float)
fsigma=np.array(fsigma,float)
psigma=np.array(psigma,float)
t10sigma=np.array(t10sigma,float)
tHIsigma=np.array(tHIsigma,float)
bp_flux=np.array(bp_flux,float)
p_flux=np.array(p_flux,float)
t10_flux=np.array(t10_flux,float)
tHI_flux=np.array(tHI_flux,float)

bmaj_f[bmaj_f==0.0]=np.nan
bmaj_p[bmaj_p==0.0]=np.nan
bmaj_t10[bmaj_t10==0.0]=np.nan
bmaj_tHI[bmaj_tHI==0.0]=np.nan
bmin_f[bmin_f==0.0]=np.nan
bmin_p[bmin_p==0.0]=np.nan
bmin_t10[bmin_t10==0.0]=np.nan
bmin_tHI[bmin_t10==0.0]=np.nan
fsigma[fsigma==0.0]=np.nan
psigma[psigma==0.0]=np.nan
t10sigma[t10sigma==0.0]=np.nan
tHIsigma[tHIsigma==0.0]=np.nan
bp_flux[bp_flux==0.0]=np.nan
p_flux[p_flux==0.0]=np.nan
t10_flux[t10_flux==0.0]=np.nan
tHI_flux[tHI_flux==0.0]=np.nan

# Plot spectra for flux cal
# logprint("Plotting Results", logfileout='logs/QA.log')
# fig=pylab.figure()
# pylab.plot(freq_f,bmaj_f,'b-')
# pylab.plot(freq_f,bmin_f,'r-')
# pylab.xlim(min(freq_f),max(freq_f))
# pylab.ylim(4.,25.)
# pylab.xlabel('Frequency [MHz]')
# pylab.ylabel('Beam size ["]')
# pylab.title('Beamsize for 3C286')
# pylab.savefig('image_fluxcal_beamsize.png')
# pylab.close(fig)
# 
# fig=pylab.figure()
# pylab.plot(freq_f,bp_flux,'k-')
# pylab.xlim(min(freq_f),max(freq_f))
# pylab.xlabel('Frequency [MHz]')
# pylab.ylabel('Flux [Jy/bm]')
# pylab.title('Flux of 3C286')
# pylab.savefig('image_fluxcal_spectrum.png')
# pylab.close(fig)
# 
# fig=pylab.figure()
# pylab.plot(freq_f,fsigma,'k-')
# pylab.xlim(min(freq_f),max(freq_f))
# pylab.xlabel('Frequency [MHz]')
# pylab.ylabel('RMS [Jy/bm]')
# pylab.title('Noise of 3C286 Image')
# pylab.savefig('image_fluxcal_rms.png')
# pylab.close(fig)

# Plot spectra for phase cal
fig=pylab.figure()
pylab.plot(freq_p,bmaj_p,'b-')
pylab.plot(freq_p,bmin_p,'r-')
pylab.xlim(min(freq_p),max(freq_p))
pylab.ylim(4.,12.)
pylab.xlabel('Frequency [MHz]')
pylab.ylabel('Beam size ["]')
pylab.title('Beamsize for J0943-0819')
pylab.savefig('image_phasecal_beamsize.png')
pylab.close(fig)

fig=pylab.figure()
pylab.plot(freq_p,p_flux,'k-')
pylab.xlim(min(freq_p),max(freq_p))
pylab.xlabel('Frequency [MHz]')
pylab.ylabel('Flux [Jy/bm]')
pylab.title('Flux of J0943-0819')
pylab.savefig('image_phasecal_spectrum.png')
pylab.close(fig)

fig=pylab.figure()
pylab.plot(freq_p,psigma,'k-')
pylab.xlim(min(freq_p),max(freq_p))
pylab.xlabel('Frequency [MHz]')
pylab.ylabel('RMS [Jy/bm]')
pylab.title('Noise of J0943-0819 Image')
pylab.savefig('image_phasecal_rms.png')
pylab.close(fig)

# Plot spectra for 10 mJy source
fig=pylab.figure()
pylab.plot(freq_t10,bmaj_t10,'b-')
pylab.plot(freq_t10,bmin_t10,'r-')
pylab.xlim(min(freq_t10),max(freq_t10))
pylab.ylim(4.,12.)
pylab.xlabel('Frequency [MHz]')
pylab.ylabel('Beam size ["]')
pylab.title('Beamsize for 10 mJy Source')
pylab.savefig('image_10mJy_beamsize.png')
pylab.close(fig)

fig=pylab.figure()
pylab.plot(freq_t10,t10_flux,'k-')
pylab.xlim(min(freq_t10),max(freq_t10))
pylab.xlabel('Frequency [MHz]')
pylab.ylabel('Flux [Jy/bm]')
pylab.title('Flux of 10 mJy source')
pylab.savefig('image_10mJy_spectrum.png')
pylab.close(fig)

fig=pylab.figure()
pylab.plot(freq_t10,t10sigma,'k-')
pylab.xlim(min(freq_t10),max(freq_t10))
pylab.xlabel('Frequency [MHz]')
pylab.ylabel('RMS [Jy/bm]')
pylab.title('Noise of 10 mJy source')
pylab.savefig('image_10mJy_rms.png')
pylab.close(fig)

# Plot spectra for HI field
fig=pylab.figure()
pylab.plot(freq_tHI,bmaj_tHI,'b-')
pylab.plot(freq_tHI,bmin_tHI,'r-')
pylab.xlim(min(freq_tHI),max(freq_tHI))
pylab.ylim(4.,12.)
pylab.xlabel('Frequency [MHz]')
pylab.ylabel('Beam size ["]')
pylab.title('Beamsize for HI field')
pylab.savefig('image_HIdet_beamsize.png')
pylab.close(fig)

fig=pylab.figure()
pylab.plot(freq_tHI,tHI_flux,'k-')
pylab.xlim(min(freq_tHI),max(freq_tHI))
pylab.xlabel('Frequency [MHz]')
pylab.ylabel('Flux [Jy/bm]')
pylab.title('Flux of HI field')
pylab.savefig('image_HIdet_spectrum.png')
pylab.close(fig)

fig=pylab.figure()
pylab.plot(freq_tHI,tHIsigma,'k-')
pylab.xlim(min(freq_tHI),max(freq_tHI))
pylab.xlabel('Frequency [MHz]')
pylab.ylabel('RMS [Jy/bm]')
pylab.title('Noise of HI field')
pylab.savefig('image_HIdet_rms.png')
pylab.close(fig)

#Move plots, images to sub-directory
os.system("mv *.png plots")
os.system("mv *cube*.* images")

#Make output webpage
if os.path.exists("QA.html"):
    os.system("rm QA.html")
wlog = open("QA.html","w")
wlog.write('<html>\n')
wlog.write('<head>\n')
wlog.write('<title>CHILES Pipeline Web Log</title>\n')
wlog.write('</head>\n')
wlog.write('<body>\n')
wlog.write('<br>\n')
wlog.write('<hr>\n')
wlog.write('<center>CHILES_pipe_QA results:</center>')
wlog.write('<li> Session: '+SDM_name+'</li>\n')
wlog.write('<li><a href="logs/QA.log">QA Log</a></li>\n')
wlog.write('<hr>\n')
wlog.write('<br>\n')
wlog.write('<li> Amp. vs. Frequency for phase calibrator showing uncalibrated masking: \n')
wlog.write('<br>\n')
wlog.write('<table> \n')
for ii in seq:
    wlog.write('<tr><img src="plots/flag1500m_Spw'+str(ii)+'.png"></tr>\n')
wlog.write('</table> \n')
wlog.write('</li>')
wlog.write('<br>')
wlog.write('<br>\n')
wlog.write('<li> Beam Size vs. Frequency for J0943-0819</li>\n')
wlog.write('<li><img src="plots/image_phasecal_beamsize.png"></li><br>\n')
wlog.write('<li> Flux vs. Frequency for J0943-0819</li>\n')
wlog.write('<li><img src="plots/image_phasecal_spectrum.png"></li><br>\n')
wlog.write('<li> RMS Noise vs. Frequency for J0943-0819</li>\n')
wlog.write('<li><img src="plots/image_phasecal_rms.png"></li><br>\n')
wlog.write('<li> Beam Size vs. Frequency for 10 mJy source</li>\n')
wlog.write('<li><img src="plots/image_10mJy_beamsize.png"></li><br>\n')
wlog.write('<li> Flux vs. Frequency for 10 mJy source</li>\n')
wlog.write('<li><img src="plots/image_10mJy_spectrum.png"></li><br>\n')
wlog.write('<li> RMS Noise vs. Frequency for 10 mJy source</li>\n')
wlog.write('<li><img src="plots/image_10mJy_rms.png"></li><br>\n')
wlog.write('<li> Beam Size vs. Frequency for HI source</li>\n')
wlog.write('<li><img src="plots/image_HIdet_beamsize.png"></li><br>\n')
wlog.write('<li> Flux vs. Frequency for HI source</li>\n')
wlog.write('<li><img src="plots/image_HIdet_spectrum.png"></li><br>\n')
wlog.write('<li> RMS Noise vs. Frequency for HI source</li>\n')
wlog.write('<li><img src="plots/image_HIdet_rms.png"></li><br>\n')
wlog.write('<br>\n')
wlog.write('<li> RMS Noise per Spw for 10 mJy source cube</li>\n')
wlog.write('<table>\n')
for ii in seq:
    wlog.write('<tr>\n')
    wlog.write('<td> Noise in spw '+str(ii)+'= '+str(sigma[ii]*1e3)+' mJy</td>\n')
    wlog.write('</tr>\n')
wlog.write('</table>\n')
wlog.write('<br>\n')
wlog.write('<hr>\n')
wlog.write('</body>\n')
wlog.write('</html>\n')
wlog.close()

os.system('mv *.html FINAL/.')

logprint ("Finished CHILES_pipe_QA.py", logfileout='logs/QA.log')
time_list=runtiming('QA', 'end')

pipeline_save()
