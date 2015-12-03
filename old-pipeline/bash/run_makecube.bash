#  _   _  ___ _____ _____
# | \ | |/ _ \_   _| ____|
# |  \| | | | || | |  _|
# | |\  | |_| || | | |___
# |_| \_|\___/ |_| |_____|
#
# The disk setup is done in the setup_disks.bash script
#
# When this is run as a user data start up script it is run as root - BE CAREFUL!!!

# Do we have an EBS volume mount and format it we present 4 volumes for makecube
if [ -b "/dev/xvdf" ]; then
    echo 'Detected EBS'
    partprobe
    mdadm --create --verbose /dev/md1 --level=0 -c256 --raid-devices=4 /dev/xvdf /dev/xvdg /dev/xvdh /dev/xvdi
    blockdev --setra 65536 /dev/md1
    mkfs.ext4 /dev/md1
    mkdir -p /mnt/data
    mount -t ext4 -o noatime /dev/md1 /mnt/data
    chmod -R 0777 /mnt/data

    mkdir -p /mnt/data/output/Chiles
    mkdir -p /mnt/data/input/Chiles
    mkdir -p /mnt/output
    mkdir -p /mnt/input

    ln -s /mnt/data/output/Chiles /mnt/output/Chiles
    ln -s /mnt/data/input/Chiles /mnt/input/Chiles
    chmod -R 0777 /mnt/output/Chiles
    chmod -R 0777 /mnt/input/Chiles

    # If we need an EBS volume we need a lot of memory so make a swap on the disk
    /bin/dd if=/dev/zero of=/mnt/output/swapfile bs=1G count={4}
    chown root:root /mnt/output/swapfile
    chmod 600 /mnt/output/swapfile
    /sbin/mkswap /mnt/output/swapfile
    /sbin/swapon /mnt/output/swapfile
else
    echo 'No EBS volume'
    # Everything on the Ephemeral drive
    mkdir -p /mnt/output/input
    chmod -R 0777 /mnt/output/input
    ln -s /mnt/output/input /mnt/input


    mkdir -p /mnt/output/Chiles
    chmod -R 0777 /mnt/output/Chiles
fi

# Log the disk usage
df -h
ls -lR /mnt

# Install the latest versions of the Python libraries and pull the latest code
pip2.7 install {1}
cd /home/ec2-user/chiles_pipeline
git pull

# Copy files from S3
python2.7 /home/ec2-user/chiles_pipeline/python/copy_makecube_input.py -p 6 {2} {3}

# Log the disk usage
df -h

# Run the make pipeline
bash -vx /home/ec2-user/chiles_pipeline/bash/start_makecube.sh {0}

# Log the disk usage
df -h

# Copy files to S3
python2.7 /home/ec2-user/chiles_pipeline/python/copy_makecube_output.py {0}

# Copy log files to S3
python2.7 /home/ec2-user/chiles_pipeline/python/copy_log_files.py -p 3 IMGCONCAT-logs/{0}

# Terminate
shutdown -h now
