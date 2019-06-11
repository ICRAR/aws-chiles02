# chiles-pipeline
README for CHILES Pipeline
October 14, 2018
Version 3.3

This is the production quality version of the pipeline.  It is
designed to run on CASA 5.3.0  It can be found in Socorro at 
/lustre/aoc/projects/chiles/chiles_pipeline.

The code in the new pipeline is based on the modified EVLA pipeline 
that was used to reduce the CHILES data in 2013 and simplified code 
that Ximena wrote to do bandpass calibration.  Unlike the EVLA 
pipeline, it is designed to run in a modular fashion with a person 
examining the output after each module is complete.  At present the 
pipeline consists of five modules:
	1.	CHILES_pipe_initial.py
	2.	CHILES_pipe_bandpass.py
	3.	CHILES_pipe_phasecal.py
	4.	CHILES_pipe_target.py
	5.	CHILES_pipe_testcubes.py
	6.	CHILES_pipe_split.py

In addition to these modules, there are some more Python scripts 
that are used when running the pipeline:
	1.	CHILES_pipe_startup.py
	2.	CHILES_pipe_restore.py
	3.	EVLA_functions.py
	4.	lib_EVLApipeutils.py

As of version 3.1 of the pipeline, the ability to run as a batch process has been included:
        1.      CHILES_pipeline.py: runs all scripts together with parameters passed in from the command line.
	2.	CHILES_pipeline_batch.py: same as above but gets parameters from chiles_parameters.list, selected via a line number.  This enables batch processing.  

Finally, there are five modules to be used only when re-running the
modules or re-making the plots.
	1.	CHILES_pipe_bandpass_rerun.py
	2.	CHILES_pipe_bandpass_plots.py
	3.	CHILES_pipe_phasecal_rerun.py
	4.	CHILES_pipe_phasecal_plots.py
	5.	CHILES_pipe_target_plots.py

Plus there is CHILES_pipe_restore.list, which contains the list of 
variables used by the pipeline.

All of these are the same as the original modules, but the rerun
modules do not do any flagging.  The plots modules just remake the
diagnostic plots, but without any calibration.  To run any of 
these, you should first quit CASA, then restart and run 
CHILES_pipe_restore.py before running the modules.


All eight of the Python CASA scripts are described in more detail 
below.

To run the pipeline: To run the pipeline, just start by running 
CHILES_pipe_initial.py, which only needs to be run once from 
the directory which contains the data to be reduced. The CASA 
command is > execfile('/path/CHILES_pipe_initial.py') where 
"path" is the full path for the pipeline code. Similar instances of 
execfile are used to run subsequent modules.  It should be possible 
to re-run any module multiple times without needing to do anything 
special.

If you quit CASA in between running modules, then you need to 
restore the pipeline state by running > execfile('/path/
CHILES_pipe_restore.py') before you run the next module.  CASA does 
not always remove caltables from memory after plotting so you will 
likely have to restart CASA when re-running modules.  

Description of latest changes:

v3.0:  	Updated to CASA 5.1.2, 
	changed from split to oldsplit (due to errors in split)
	changed flagging to clip spw=14, then determine RMS for RFLAG, then flag.
		no EXTEND on flagging routines anymore
		time-averaging is still being done for the target (using preset RMS)


Detailed description of CHILES pipeline scripts:

	•	CHILES_pipe_initial.py: This script does all of the pipeline setup and those tasks that do not need to be repeated or lack user input.  It begins by defining the path for the pipeline and the location of the weblogs from the EVLA continuum pipeline that is automatically run on all data.  It sets up the CASA log and the timing log and then runs CHILES_pipe_startup (see the next bullet for its description).  At this point, if needed, it reads in the SDM file and creates a measurement set (MS).  The user may specify a subset of scans to import if desired.  Online flagging and zero flagging is done when importing the SDM.  If a MS exists, zero flagging is done and the user is reminded that the online flags need to be applied.  If Hanning smoothing is requested by the user, it then smooths the data.  If Hanning smoothing is requested by the user, it then smooths the data.  It then runs "listobs" to produce a listing that describes the entire data set.  The code then automatically identifies all the spws, fields observed, observing intents, and number of antennas.  The integration time is hardcoded in this section as it remains at 8s for all CHILES observations.  The CASA version of the AIPS task PRTAN is run to help the user select a reference antenna.  The code then does all flagging that does not require user input (or doesn't need to be repeated later).  This includes flagging antennas identified as bad by the user, flagging the continuum spws, those data that are exactly zero, and shadowed data.  The data is then quacked and all flags are applied and saved.  The script concludes by producing a webpage (initial.html) that has the logs, the PRTAN output, and a link to the EVLA pipeline weblogs.  It will now link to the weblogs for data from phase 1-3.  This script is based on the initial code from Ximena's bandpass script as well as the initial data processing in the EVLA pipeline. 

	•	CHILES_pipe_startup.py: This script is designed to setup the pipeline as it begins.  It takes user input for the name of the SDM file.  If a MS has already been created, it will identify that file and skip creating a new MS file.  It will ask the user to identify any bad antennas that should be flagged in all the data, and it will ask the user to identify a reference antenna; a list of antennas can be provided.  If the user does not know which antenna should be a reference antenna, this step can be skipped and the user will be prompted again later in the pipeline.  The user can provide a ranked list of reference antennas so that if the primary antenna is flagged, the pipeline will select the next listed antenna as a reference.  Otherwise, the CASA code has its own algorithm for identifying alternate reference antennas that may not be optimal for our purposes.  Most of this script comes from the EVLA_pipe_startup.py script. 

	•	CHILES_pipe_bandpass.py: This script starts by removing any past calibration tables (including results from past 'setjy' runs using 'delmod') and defining variables containing the spectral windows and channels to be used for the calibration.  The script the flags the traditionally RFI-affected channels of the BP calibrator.  The main body of the script then runs 'setjy' for 3C286.  Calibration tables for the gain curves and the antenna positions are created with gencal.  Then the initial gain (phases only) and delay calibrations are done to be used for the initial bandpass calibration. The bandpass (BP) calibration is done with fillgaps=0 so no interpolation.  These calibrations are then applied (using default linear interpolation in time, frequency).  Flagging occurs by clipping the source at the 50 Jy level, then using RFLAG to determine the noise in spw=14 for a subsequent RFLAG run on all the spws.  The flags are not extended.  At this point, the initial gain (phase only) and delay calibrations are repeated before a final bandpass calibration is done.  For delay calibration, only the central 33% of the channels are used, while all other calibration uses all but the outer 50 channels.  To assist with removing RFI from the calibration data, we also set a uvrange='>1500m' to exclude the worst RFI on short baselines and the minimum number of baselines for a solution is set to 8.  To get a solution a minimum SNR of 3 is required for all calibration tasks, except the bandpass calibration, which requires 5.  The calibration is then applied to 3C286 and the amplitude vs. phase is plotted for the fluxcalibrator along with a time-averaged and baseline-averaged spectrum.  3C286 is then imaged and the beamsize, peak flux, and rms are measured.  The flags are backed up after the applycal.  The results are plotted up and embedded into a webpage (bandpass.html) for the user to examine.  Most of this script is based on the code written by Ximena to test bandpass calibration techniques.  If the bandpass solutions are not good enough, the user can manually flag the 3C286 data or the solutions and re-run this module. 

	•	CHILES_pipe_phasecal.py: The script begins by removing old calibration tables and old images of the phase calibrator (if the task was previously run).  The same uvrange and minimum number of baselines needed for a solution from the bandpass module are used here as well.  The same RFI flagging of channels is done to the phase calibrator as well.  To get a solution a minimum SNR of 8 is required for all calibration tasks.  All but the 50 edge channels are used for the gain solutions.  The first stage is to do a full complex gain (amplitude and phases) for both 3C286 and the phase calibrator (J0943-0819).  The solution interval is chosen to be the scan length to maximize signal-to-noise.  The flux scaling is then done using the code from the EVLA_pipe_fluxboot script boot-strapping the flux scale from 3C286 to J0943.  All of the final calibrations (antpos, gain_curves, delay, bandpass) and the initial gain and bootstrapping solutions are applied.  Flagging occurs by clipping the source at the 30 Jy level, then using RFLAG to determine the noise in spw=14 for a subsequent RFLAG run on all the spws.  The flags are not extended..  The complex gain and boot-strapped fluxes are then re-derived after the flagging to produce the final solutions, which are applied to both calibrators.  Delmod is run (on field='0') before the second 'setjy' and after the final applycal is run to avoid 'setjy' appending values to the existing models.  The flags are backed up after the phase solutions are applied in applycal.  The same calibration plots as in bandpass are generated and put on a webpage (phasecal.html) for the user to inspect.  If the calibrations are not sufficiently good, the user can manually flag J0943 and re-run these calibrations.  There should be no need to flag 3C286 again. 

	•	CHILES_pipe_target.py: This module is extremely simple. It runs an applycal for the deepfield.  Flagging occurs as above with a clip at 30 Jy, then using RFLAG to determine the noise in spw=14 for a subsequent RFLAG run on all the spws.  The flags are not extended.  Finally, any channel that is more than 90% flagged is completely flagged (code from Ximena).  The flags are backed up at this point.  The target data is then split off to form a continuum image, which is imaged and the beamsize, peak flux, and rms is measured for every spectral window.  Plots of amplitude vs. frequency and vs. time are generated for the deepfield along with a plot of the fraction of flagged data vs. frequency.  The results are presented on another webpage (target.html) for perusal.  At this point, the user should be able to go in and manually flag the target knowing that the calibration is good.  This task does not need to be re-run. 

	•	CHILES_pipe_testcubes.py: This module starts by splitting off the data for the bandpass and phase calibrators and the deepfield for each spectral window.  It then uses this split data to make small cubes for each spw for the flux and phase calibrator and the deepfield centered on the bright 10 mJy continuum source and the brightest HI detection at low-z.  All cubes span the entire 2048 channel range covered by each spw in the observation, but are only 64x64 pixels in area.  After both cubes are made, then a spectrum is extracted covering the full spectral range towards the 10 mJy, and two integrated around the bright HI detection (one spanning the full spectral range and the other zoomed in on the detection). A webpage containing these spectra is generated at the end.  The noise in the HI detection cube is printed as well.  This module only needs to be re-run if additional flagging is done by hand and new test cubes are required.  

	•	CHILES_pipe_split.py: This last module re-calculates the flagging statistics and backs up the final flags.  It saves the flagging statistics in a text file for future reference.  It splits off the deepfield data.  It copies this MS plus the output from the final flagmanager, the flag commands, and all the calibration tables to the FINAL subdirectory.  It also saves all of the weblogs.  The code generates a final flagging percentage vs. frequency plot which is shown on the resulting split.html webpage.  

To run this code locally (or in Socorro), the path to the pipeline 
and to the weblogs from the NRAO pipeline have to be added to (or 
uncommented) in the CHILES_pipe_initial.py, CHILES_pipeline, or CHILES_pipeline_batch modules.

