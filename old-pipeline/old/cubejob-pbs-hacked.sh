#PBS -l nodes=1:ppn=6:compute
#PBS -l walltime=10:00:00
#PBS -N test_cube
#PBS -A rdodson
#PBS -t 0-5
#PBS -o $HOME/Tests/Chiles/run-11.out
#PBS -e $HOME/Tests/Chiles/run-11.err

# For non-PBS testing
export PBS_JOBID=9999
export PBS_ARRAYID=0
#for PBS_ARRAYID in 0 1 2 3
#
# each run has a unique id (converted from pbs_job_id, e.g.19625[0].pleiades.icrar.org)
export CH_RUN_ID=${PBS_JOBID}
# total number of jobs
export CH_NUM_JOB=4 ## Should match total from ARRAY (-t) line 
# target field
export CH_TARGET_FIELD='deepfield'

export CH_OBS_DIR=$HOME/Data
# the index of the first / last observation (to be split) listed in "ls -l $OBS_DIR"
export CH_OBS_FIRST=0
export CH_OBS_LAST=3

#	  SpwID  Name           #Chans   Frame   Ch0(MHz)  ChanWid(kHz)  TotBW(kHz) BBC Num  Corrs  
#	  0      EVLA_L#A0C0#0    2048   TOPO     951.000        15.625     32000.0      12  RR  LL
#	  1      EVLA_L#A0C0#1    2048   TOPO     983.000        15.625     32000.0      12  RR  LL
#	  2      EVLA_L#A0C0#2    2048   TOPO    1015.000        15.625     32000.0      12  RR  LL
#	  3      EVLA_L#A0C0#3    2048   TOPO    1047.000        15.625     32000.0      12  RR  LL
#	  4      EVLA_L#A0C0#4    2048   TOPO    1079.000        15.625     32000.0      12  RR  LL
#	  5      EVLA_L#A0C0#5    2048   TOPO    1111.000        15.625     32000.0      12  RR  LL
#	  6      EVLA_L#A0C0#6    2048   TOPO    1143.000        15.625     32000.0      12  RR  LL
#	  7      EVLA_L#A0C0#7    2048   TOPO    1175.000        15.625     32000.0      12  RR  LL
#	  8      EVLA_L#A0C0#8    2048   TOPO    1207.000        15.625     32000.0      12  RR  LL
#	  9      EVLA_L#A0C0#9    2048   TOPO    1239.000        15.625     32000.0      12  RR  LL
#	  10     EVLA_L#A0C0#10   2048   TOPO    1271.000        15.625     32000.0      12  RR  LL
#	  11     EVLA_L#A0C0#11   2048   TOPO    1303.000        15.625     32000.0      12  RR  LL
#	  12     EVLA_L#A0C0#12   2048   TOPO    1335.000        15.625     32000.0      12  RR  LL
#	  13     EVLA_L#A0C0#13   2048   TOPO    1367.000        15.625     32000.0      12  RR  LL
#	  14     EVLA_L#A0C0#14   2048   TOPO    1399.000        15.625     32000.0      12  RR  LL

export CH_SLCT_FREQ=1 # whether to select frequencies, e.g. either spw = 1:10~11Mhz or spw = 1
export CH_SPW=14 # ALL, 1~2, etc.ALL means '*'
export CH_FREQ_MIN=1407 # MHz -- must be int (could be float if required, but makecube would need changing)
export CH_FREQ_MAX=1419 # MHz -- must be int
export CH_FREQ_STEP=4   # MHz -- must be int
export CH_FREQ_WIDTH=15.625 # in kHz -- float value

export CH_MODE_DEBUG=0 # In the debug mode, CASA routines are not called but only printed
export CH_BKP_SPLIT=0 # whether to make a backup copy of the split vis files

# each obs will create a sub-directory under this
export CH_VIS_DIR=$HOME/Data/Outputs/Chiles/split_vis
export CH_VIS_BK_DIR=$HOME/Data/Outputs/Chiles/backup_split_vis

# NOTE - ON pleiades, do not set this to /scratch
export CH_CUBE_DIR=$HOME/Data/Outputs/Chiles/split_cubes
#export CH_OUT_NAME=$HOME/Tests/Chiles/cubes/comb_$CH_FREQ_MIN~$CH_FREQ_MAX.image
export CH_OUT_DIR=$HOME/Data/Outputs/Chiles/cubes

export CH_SPLIT_TIMEOUT=3600 # 1 hour
export CH_CLEAN_TIMEOUT=3600

# create a separate casa_work directory for each casa process
# NOTE - ON pleiades, do not set this to /scratch
export CH_CASA_WORK_DIR=$HOME/Tests/Chiles/casa_work_dir
# TODO - create it if not there
mkdir -p $CH_CASA_WORK_DIR/${PBS_JOBID}[${PBS_ARRAYID}]
cd $CH_CASA_WORK_DIR/${PBS_JOBID}[${PBS_ARRAYID}]

# point to casapy installation
#CH_CASA_SOURCE=/mnt/gleam/software/casapy-42.0.28322-021-1-64b
#CH_CASA_SOURCE=/home/apopping/Software/casapy-42.1.29047-001-1-64b
#CH_CASA_SOURCE=/home/apopping/Software/casapy-41.0.24668-001-64b-2
#CH_CASA_SOURCE=/home/rdodson/Software/Casa/casapy-42.1.29047-001-1-64b
# run casapy
casapy --nologger  --log2term --logfile casapy.log  -c ../../makecube.py &
#done
