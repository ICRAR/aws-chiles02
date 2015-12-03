#!/bin/bash
export CH_JOB_ID=0
export CH_RUN_ID=0
export CH_NUM_JOB=0

# target field
export CH_TARGET_FIELD='deepfield'

export CH_OBS_DIR=/mnt/output/Chiles/split_vis
export CH_OBS_FIRST=0
export CH_OBS_LAST=0

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
export CH_SPW='' # ALL, 1~2, etc.ALL means '*'
export CH_FREQ_MIN=$1 # MHz -- must be int (could be float if required, but makecube would need changing)
export CH_FREQ_MAX=$2 # MHz -- must be int
export CH_FREQ_STEP=4   # MHz -- must be int
export CH_FREQ_WIDTH=15.625 # in kHz -- float value

export CH_MODE_DEBUG=0 # In the debug mode, CASA routines are not called but only printed
export CH_BKP_SPLIT=0 # whether to make a backup copy of the split vis files

export CH_CUBE_DIR=/mnt/data
export CH_OUT_DIR=/mnt/data/Chiles/cubes

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

# run casapy
casapy --nologger  --log2term --logfile casapy.log  -c /home/ec2-user/chiles_pipeline/python/loop_clean_all.py
#done

