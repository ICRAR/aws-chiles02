#!/bin/bash -vx
#  _   _  ___ _____ _____
# | \ | |/ _ \_   _| ____|
# |  \| | | | || | |  _|
# | |\  | |_| || | | |___
# |_| \_|\___/ |_| |_____|
#
# When this is run as a user data start up script is is run as root - BE CAREFUL!!!
# Setup the ephemeral disks

# Print into the logs the disk free
df -h

if [ -b "/dev/xvdb" ]; then

    METADATA_URL_BASE="http://169.254.169.254/latest"

    yum -y -d0 install mdadm curl

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
    if (( ephemeral_count > 1 )); then
        umount /media/ephemeral0
        # overwrite first few blocks in case there is a filesystem, otherwise mdadm will prompt for input
        for drive in $drives; do
          dd if=/dev/zero of=$drive bs=4096 count=1024
        done

        partprobe
        mdadm --create --verbose /dev/md0 --level=0 -c256 --raid-devices=$ephemeral_count $drives
        blockdev --setra 65536 /dev/md0
        mkfs.ext4 /dev/md0
        mkdir -p /mnt/output
        mount -t ext4 -o noatime /dev/md0 /mnt/output
    elif (( ephemeral_count == 1 )); then
        if mountpoint -q "/media/ephemeral0" ; then
            # The ephemeral disk is usually mounted on /media/ephemeral0
            rm -f /mnt/output
            ln -s /media/ephemeral0 /mnt/output
        else
            # The ephemeral disk is not mounted on /media/ephemeral0 so mount it
            mkdir -p /mnt/output
            mkfs.ext4 /dev/xvdb
            mount /dev/xvdb /mnt/output
        fi
    else
        mkdir -p /mnt/output
        mkfs.ext4 /dev/xvdb
        mount /dev/xvdb /mnt/output
    fi
fi
chmod -R 0777 /mnt/output

# Print into the logs the disk free
df -h
