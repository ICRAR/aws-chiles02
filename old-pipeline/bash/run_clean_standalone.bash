#  _   _  ___ _____ _____
# | \ | |/ _ \_   _| ____|
# |  \| | | | || | |  _|
# | |\  | |_| || | | |___
# |_| \_|\___/ |_| |_____|
#
# The disk setup is done in the setup_disks.bash script
#
# When this is run as a user data start up script it is run as root - BE CAREFUL!!!

# Clean uses a lot of memory so make a swap on the disk
/bin/dd if=/dev/zero of=/mnt/output/swapfile bs=1G count={1}
chown root:root /mnt/output/swapfile
chmod 600 /mnt/output/swapfile
/sbin/mkswap /mnt/output/swapfile
/sbin/swapon /mnt/output/swapfile

# We need lots and lots of files open for the clean process
ulimit -n 8192

# Install the latest versions of the Python libraries and pull the latest code
pip2.7 install {2}
cd /home/ec2-user/chiles_pipeline
git pull

# create a separate casa_work directory for each casa process
export CH_CASA_WORK_DIR=/home/ec2-user/Chiles/casa_work_dir

mkdir -p ${{CH_CASA_WORK_DIR}}/1020-1024
cd ${{CH_CASA_WORK_DIR}}/1020-1024

export HOME=/home/ec2-user
export USER=root
# point to casapy installation
export PATH=$PATH:/home/ec2-user/casapy-42.2.30986-1-64b/bin
export PYTHONPATH=${{PYTHONPATH}}:/home/ec2-user/chiles_pipeline/python:/home/ec2-user/chiles_pipeline/standalone

# Copy files from S3
python2.7 /home/ec2-user/chiles_pipeline/standalone/copy_clean_input_standalone.py {0} -p 4

# Log the disk usage
df -h

# Run the clean pipeline
# run casapy
#casapy --nologger --log2term --logfile casapy.log  -c /home/ec2-user/chiles_pipeline/standalone/standalone_clean.py
python2.7 /home/ec2-user/chiles_pipeline/python/launch_trace2.py casapy --nologger --log2term --logfile casapy.log  -c /home/ec2-user/chiles_pipeline/standalone/standalone_clean.py

# Log the disk usage
df -h

# Copy files to S3
python2.7 /home/ec2-user/chiles_pipeline/python/copy_log_files.py -p 3 CLEAN-log/standalone/{0}

# Terminate
shutdown -h now
