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
      lvcreate -L 10G --name swap dfms-group
      docker-storage-setup
      lvcreate --extents 100%FREE --name data dfms-group

      mkfs.xfs /dev/dfms-group/data
      mkdir -p /mnt/dfms
      mount /dev/dfms-group/data /mnt/dfms

      mkswap /dev/dfms-group/swap
      swapon /dev/dfms-group/swap
    else
      mkdir -p /mnt/dfms
      mkfs.xfs /dev/xvdb

      mount /dev/xvdb /mnt/dfms
      dd if=/dev/zero of=/mnt/swapfile bs=1M count=1024
      mkswap /mnt/swapfile
      swapon /mnt/swapfile
      chmod 0600 /mnt/swapfile
    fi
fi
# Print free disk space
df -h

# Create the DFMS root
mkdir -p /mnt/dfms/dfms_root
chmod -R 0777 /mnt/dfms

rm -rf /var/lib/docker
service docker start
sleep 10

#docker login --email=a@b.com --username=icrar --password=XXX sdp-docker-registry.icrar.uwa.edu.au:8080

# Get the docker containers now to prevent a race condition later
#docker pull sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/java-s3-copy:latest
#docker pull sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/chiles02:latest
docker pull kevinvinsen/java-s3-copy:latest
docker pull kevinvinsen/chiles02:latest

cd /home/ec2-user
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && git pull'
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && python setup.py install'
runuser -l ec2-user -c 'cd /home/ec2-user && git clone https://github.com/ICRAR/aws-chiles02.git'

cat /home/ec2-user/.ssh/id_dfms.pub >> /home/ec2-user/.ssh/authorized_keys
runuser -l ec2-user -c 'cd /home/ec2-user/aws-chiles02/pipeline/aws_chiles02 && source /home/ec2-user/virtualenv/aws-chiles02/bin/activate && python startup_complete.py startup_complete us-west-2'
runuser -l ec2-user -c 'cd /home/ec2-user/dfms && source /home/ec2-user/virtualenv/dfms/bin/activate && nohup dfmsNM --rest -v --dfms-path=/home/ec2-user/aws-chiles02/pipeline --id=kv -H 0.0.0.0 > /mnt/dfms/dfms_root/logfile.log 2>&1 &'
