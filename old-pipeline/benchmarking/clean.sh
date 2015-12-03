#PBS -l nodes=1:ppn=4,mem=35gb:compute
#PBS -l walltime=10:00:00
#PBS -N clean_44
#PBS -A rdodson

cd /mnt/hidata/test/src

python2.7 launch_trace.py ~/Software/Casa/casapy-test-43.0.30606-002-1-64b/casapy --nologger -c clean_44.py
