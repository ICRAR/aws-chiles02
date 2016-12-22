#!/bin/bash -vx
yum -y update

# Print into the logs the disk free
df -h

# Move the docker volumes to the ephemeral drive
service docker stop
sleep 10

if [ -b "/dev/xvdb" ]; then

    METADATA_URL_BASE="http://169.254.169.254/latest"

    yum -y -d0 install docker-storage-setup curl

    # Configure Raid if needed - taking into account xvdb or sdb
    root_drive=`df -h | grep -v grep | awk 'NR==2{print $1}'`

    if [ "$root_drive" == "/dev/xvda1" ]; then
      echo "Detected 'xvd' drive naming scheme (root: $root_drive)"
      DRIVE_SCHEME='xvd'
    else
      echo "Detected 'sd' drive naming scheme (root: $root_drive)"
      DRIVE_SCHEME='sd'
    fi

    # figure out how many ephemerals we have by querying the metadata API, and then:
    #  - convert the drive name returned from the API to the hosts DRIVE_SCHEME, if necessary
    #  - verify a matching device is available in /dev/
    drives=""
    ephemeral_count=0
    ephemerals=$(curl --silent $METADATA_URL_BASE/meta-data/block-device-mapping/ | grep ephemeral)
    for e in $ephemerals; do
      echo "Probing $e .."
      device_name=$(curl --silent $METADATA_URL_BASE/meta-data/block-device-mapping/$e)
      # might have to convert 'sdb' -> 'xvdb'
      device_name=$(echo $device_name | sed "s/sd/$DRIVE_SCHEME/")
      device_path="/dev/$device_name"

      # test that the device actually exists since you can request more ephemeral drives than are available
      # for an instance type and the meta-data API will happily tell you it exists when it really does not.
      if [ -b $device_path ]; then
        echo "Detected ephemeral disk: $device_path"
        drives="$drives $device_path"
        ephemeral_count=$((ephemeral_count + 1 ))
      else
        echo "Ephemeral disk $e, $device_path is not present. skipping"
      fi
    done

    echo "ephemeral_count = $ephemeral_count"
    if (( ephemeral_count >= 1 )); then
      if mountpoint -q "/media/ephemeral0" ; then
        umount /media/ephemeral0
      fi
      # overwrite first few blocks in case there is a filesystem, otherwise mdadm will prompt for input
      for drive in $drives; do
        dd if=/dev/zero of=$drive bs=4096 count=1024
      done

      if (( ephemeral_count > 1 )); then
        mdadm --create --verbose /dev/md0 --level=0 -c256 --raid-devices=$ephemeral_count $drives
        blockdev --setra 65536 /dev/md0
        pvcreate /dev/md0
        vgcreate dfms-group /dev/md0
      else
        pvcreate $drives
        vgcreate dfms-group $drives
      fi
      lvcreate -L 20G --name swap dfms-group
      docker-storage-setup
      lvcreate --extents 100%FREE --name data dfms-group

      mkfs.xfs -K /dev/dfms-group/data
#      mkfs.ext4 /dev/dfms-group/data
      mkdir -p /mnt/dfms
      mount /dev/dfms-group/data /mnt/dfms

      mkswap /dev/dfms-group/swap
      swapon /dev/dfms-group/swap
    else
      mkdir -p /mnt/dfms
      mkfs.xfs -K /dev/xvdb

      mount /dev/xvdb /mnt/dfms
      dd if=/dev/zero of=/mnt/swapfile bs=1M count=1024
      mkswap /mnt/swapfile
      swapon /mnt/swapfile
      chmod 0600 /mnt/swapfile
    fi
fi
# Print free disk space
df -h

# More file handles
ulimit -n 20480

# Create the DFMS root
mkdir -p /mnt/dfms/dfms_root
chmod -R 0777 /mnt/dfms

rm -rf /var/lib/docker
service docker start
sleep 10

# Docker 1.11.2 has some odd issues so we need to restart the server
service docker restart
sleep 10

# Get the docker containers now to prevent a race condition later
% if chiles:
docker pull kevinvinsen/chiles02:latest

# Check it works
docker run kevinvinsen/chiles02:latest /bin/echo 'Hello chiles02 container'
% endif

% if jpeg2000:
docker pull jtmalarecki/sv:latest

# Check it works
docker run jtmalarecki/sv /bin/echo 'Hello sv container'
% endif

cd /home/ec2-user
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && git pull'
runuser -l ec2-user -c 'cd /home/ec2-user && git clone https://github.com/ICRAR/aws-chiles02.git'
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && python setup.py install'
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && pip install --upgrade -r /home/ec2-user/aws-chiles02/pipeline/pip/requirements.txt'
runuser -l ec2-user -c 'cd /home/ec2-user/aws-chiles02 && source /home/ec2-user/virtualenv/aws-chiles02/bin/activate && pip install --upgrade -r /home/ec2-user/aws-chiles02/pipeline/pip/requirements.txt'

cat /home/ec2-user/.ssh/id_dfms.pub >> /home/ec2-user/.ssh/authorized_keys
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && dfmsNM --daemon -${log_level} --dfms-path=/home/ec2-user/aws-chiles02/pipeline -H 0.0.0.0 --log-dir /mnt/dfms/dfms_root --error-listener=aws_chiles02.error_handling.ErrorListener --max-request-size ${max_request_size}'
sleep 10
runuser -l ec2-user -c 'cd /home/ec2-user/aws-chiles02/pipeline/aws_chiles02 && source /home/ec2-user/virtualenv/aws-chiles02/bin/activate && python startup_complete.py ${queue} ${region} "${uuid}"'
