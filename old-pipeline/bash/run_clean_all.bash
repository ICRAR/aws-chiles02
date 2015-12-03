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
/bin/dd if=/dev/zero of=/mnt/output/swapfile bs=1G count={3}
chown root:root /mnt/output/swapfile
chmod 600 /mnt/output/swapfile
/sbin/mkswap /mnt/output/swapfile
/sbin/swapon /mnt/output/swapfile

# We need lots and lots of files open for the clean process
ulimit -n 8192

# Build the result area
mdadm --create --verbose /dev/md1 --level=0 -c256 --raid-devices=8 /dev/xvdd /dev/xvde /dev/xvdf /dev/xvdg /dev/xvdh /dev/xvdi /dev/xvdj /dev/xvdk
blockdev --setra 65536 /dev/md1
mkfs.ext4 /dev/md1
mkdir -p /mnt/data
chmod -R 0777 /mnt/data
mount -t ext4 -o noatime /dev/md1 /mnt/data

# Mount the sources
mkdir -p /mnt/input01
mkdir -p /mnt/input02
mkdir -p /mnt/input03
mkdir -p /mnt/input04
mkdir -p /mnt/input05
mkdir -p /mnt/input06

chmod -R 0777 /mnt/input01
chmod -R 0777 /mnt/input02
chmod -R 0777 /mnt/input03
chmod -R 0777 /mnt/input04
chmod -R 0777 /mnt/input05
chmod -R 0777 /mnt/input06

mount /dev/xvdl /mnt/input01
mount /dev/xvdm /mnt/input02
mount /dev/xvdn /mnt/input03
mount /dev/xvdo /mnt/input04
mount /dev/xvdp /mnt/input05
mount /dev/xvdq /mnt/input06

chmod -R 0777 /mnt/input01
chmod -R 0777 /mnt/input02
chmod -R 0777 /mnt/input03
chmod -R 0777 /mnt/input04
chmod -R 0777 /mnt/input05
chmod -R 0777 /mnt/input06

# Install the latest versions of the Python libraries and pull the latest code
pip2.7 install {4}
cd /home/ec2-user/chiles_pipeline
runuser -l ec2-user -c '(cd /home/ec2-user/chiles_pipeline ; git pull)'

# Log the disk usage
df -h

# Run the clean pipeline
runuser -l ec2-user -c 'bash -vx /home/ec2-user/chiles_pipeline/bash/start_clean_all.sh {1} {2}'

# Log the disk usage
df -h

# Copy files to S3
#runuser -l ec2-user -c 'python2.7 /home/ec2-user/chiles_pipeline/python/copy_clean_output.py {0}'

# Copy files to S3
#runuser -l ec2-user -c 'python2.7 /home/ec2-user/chiles_pipeline/python/copy_log_files.py -p 3 CLEAN-log/{0}'

# Terminate
#shutdown -h now
