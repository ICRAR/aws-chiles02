#  _   _  ___ _____ _____
# | \ | |/ _ \_   _| ____|
# |  \| | | | || | |  _|
# | |\  | |_| || | | |___
# |_| \_|\___/ |_| |_____|
#
# The disk setup is done in the setup_disks.bash script
#
# When this is run as a user data start up script it is run as root - BE CAREFUL!!!

# As we might need to wait for the mount point to arrive as it can only be attached
# after the instance is running
sleep 10
while [ ! -b "/dev/xvdf" ]; do
    echo Sleeping
    sleep 30
done

# Now mount the data disk
mkdir -p /mnt/Data/data1
mount /dev/xvdf /mnt/Data/data1
chmod -R oug+r /mnt/Data/data1

# Install the latest versions of the Python libraries and pull the latest code
pip2.7 install {2}
cd /home/ec2-user/chiles_pipeline
git pull

# Run the split pipeline
# create a separate casa_work directory for each casa process
export CH_CASA_WORK_DIR=/home/ec2-user/Chiles/casa_work_dir

mkdir -p ${{CH_CASA_WORK_DIR}}/1020-1024
cd ${{CH_CASA_WORK_DIR}}/1020-1024

# point to casapy installation
export PATH=$PATH:/home/ec2-user/casapy-42.2.30986-1-64b/bin
export PYTHONPATH=${{PYTHONPATH}}:/home/ec2-user/chiles_pipeline/python:/home/ec2-user/chiles_pipeline/standalone
export HOME=/home/ec2-user
export USER=root

# run casapy
#casapy --nologger  --log2term --logfile casapy.log  -c /home/ec2-user/chiles_pipeline/standalone/standalone_split.py
python2.7 /home/ec2-user/chiles_pipeline/python/launch_trace2.py casapy --nologger  --log2term --logfile casapy.log  -c /home/ec2-user/chiles_pipeline/standalone/standalone_split.py

# Log the disk usage
df -h

# Copy log files to S3
python2.7 /home/ec2-user/chiles_pipeline/python/copy_log_files.py -p 3 CVEL-logs/standalone/{1}

# Unattach the volume and delete it
umount /dev/xvdf
sleep 10
python2.7 /home/ec2-user/chiles_pipeline/python/delete_volumes.py {0}

# Terminate
shutdown -h now
