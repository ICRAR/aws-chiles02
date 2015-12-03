#!/bin/bash
export CLEAN_NUMBER=$3

export CH_TARGET_FIELD='deepfield'

export CH_OBS_DIR=/mnt/output/Chiles/split_vis
export CH_OBS_FIRST=0
export CH_OBS_LAST=0

export CH_SLCT_FREQ=1 # whether to select frequencies, e.g. either spw = 1:10~11Mhz or spw = 1
export CH_SPW='' # ALL, 1~2, etc.ALL means '*'
export CH_FREQ_MIN=$1 # MHz -- must be int (could be float if required, but makecube would need changing)
export CH_FREQ_MAX=$2 # MHz -- must be int
export CH_FREQ_STEP=4   # MHz -- must be int
export CH_FREQ_WIDTH=15.625 # in kHz -- float value

export CH_MODE_DEBUG=0 # In the debug mode, CASA routines are not called but only printed
export CH_BKP_SPLIT=0 # whether to make a backup copy of the split vis files

# each obs will create a sub-directory under this
export CH_VIS_DIR=/mnt/output/Chiles/split_vis
export CH_VIS_BK_DIR=/mnt/output/Chiles/backup_split_vis

# NOTE - ON pleiades, do not set this to /scratch
export CH_CUBE_DIR=/mnt/output/Chiles/split_cubes
export CH_OUT_DIR=/mnt/output/Chiles/cubes

export CH_SPLIT_TIMEOUT=3600 # 1 hour
export CH_CLEAN_TIMEOUT=3600

# create a separate casa_work directory for each casa process
export CH_CASA_WORK_DIR=$HOME/Chiles/casa_work_dir

mkdir -p ${CH_CASA_WORK_DIR}/${CH_FREQ_MIN}-${CH_FREQ_MAX}
cd ${CH_CASA_WORK_DIR}/${CH_FREQ_MIN}-${CH_FREQ_MAX}

# point to casapy installation
export PATH=$PATH:/home/ec2-user/casapy-42.2.30986-1-64b/bin
export PYTHONPATH=${PYTHONPATH}:/home/ec2-user/chiles_pipeline/python
export HOME=/home/ec2-user
export USER=root

casapy --nologger  --log2term --logfile casapy.log  -c /home/ec2-user/chiles_pipeline/python/casapy_run_imstat.py
