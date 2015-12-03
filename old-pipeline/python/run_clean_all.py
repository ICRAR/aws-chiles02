#
#    (c) UWA, The University of Western Australia
#    M468/35 Stirling Hwy
#    Perth WA 6009
#    Australia
#
#    Copyright by UWA, 2012-2015
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
"""
Start a number of CLEAN servers
"""
import argparse
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import getpass
import sys
from boto.ec2 import blockdevicemapping
from boto.ec2.blockdevicemapping import BlockDeviceType

from common import get_script, get_cloud_init, LOGGER
from echo import echo
from settings_file import AWS_AMI_ID, BASH_SCRIPT_SETUP_DISKS, AWS_INSTANCES, PIP_PACKAGES, BASH_SCRIPT_CLEAN_ALL
from ec2_helper import EC2Helper


LOGGER.info('PYTHONPATH = {0}'.format(sys.path))
SCRATCH_DISKS = 8
SCRATCH_DISK_SIZE = 200


def get_mime_encoded_user_data(instance_details, setup_disks, user_data):
    """
    AWS allows for a multipart m
    """
    # Split the frequencies
    min_freq = 940
    max_freq = 1424
    LOGGER.info('min_freq: {0}, max_freq: {1}'.format(min_freq, max_freq))

    # Build the mime message
    mime_data = MIMEMultipart()
    mime_data.attach(get_cloud_init())

    swap_size = get_swap_size(instance_details)
    data_formatted = user_data.format('TODO', min_freq, max_freq, swap_size, PIP_PACKAGES)
    mime_data.attach(MIMEText(setup_disks + data_formatted))
    return mime_data.as_string()


def get_swap_size(instance_details):
    ephemeral_size = instance_details.number_disks * instance_details.size
    if ephemeral_size > 100:
        return 8
    elif ephemeral_size > 32:
        return 2
    else:
        return 1


def start_server(
        processes,
        ami_id,
        user_data,
        setup_disks,
        instance_type,
        snapshot_ids,
        created_by,
        name,
        instance_details,
        spot_price):

    LOGGER.info('snapshot_ids: {0}'.format(snapshot_ids))
    ec2_helper = EC2Helper()
    zone = ec2_helper.get_cheapest_spot_price(instance_type, spot_price)

    if zone is not None:
        # Get the snapshot details
        connection = ec2_helper.ec2_connection
        snapshot_details = {}
        for snapshot in connection.get_all_snapshots(owner='self'):
            name = snapshot.tags.get('Name')
            if name is None:
                LOGGER.info('Looking at {0} - None'.format(snapshot.id))
            elif snapshot.status == 'completed':
                LOGGER.info('Looking at {0} - {1}'.format(snapshot.id, snapshot.tags['Name']))
                if snapshot.tags['Name'].endswith('_FINAL_PRODUCTS'):
                    snapshot_details[snapshot.id] = snapshot
            else:
                LOGGER.info('Looking at {0} - {1} which is {2}'.format(snapshot.id, snapshot.tags['Name'], snapshot.status))

        # Create the volumes
        bdm = blockdevicemapping.BlockDeviceMapping()

        # The ephemeral disk
        xvdb = BlockDeviceType()
        xvdb.ephemeral_name = 'ephemeral0'
        bdm['/dev/xvdb'] = xvdb

        if instance_details.number_disks == 2:
            xvdc = BlockDeviceType()
            xvdc.ephemeral_name = 'ephemeral1'
            bdm['/dev/xvdc'] = xvdc

        # The Scratch space
        for disks in range(0, SCRATCH_DISKS):
            xvd_n = blockdevicemapping.EBSBlockDeviceType(delete_on_termination=True)
            xvd_n.size = int(SCRATCH_DISK_SIZE)  # size in Gigabytes
            xvd_n.volume_type = 'gp2'
            last_char = chr(ord('d') + disks)
            bdm['/dev/xvd' + last_char] = xvd_n

        disks = 0
        for snapshot_id in snapshot_ids:
            snapshot_detail = snapshot_details.get(snapshot_id)
            xvd_n = blockdevicemapping.EBSBlockDeviceType(delete_on_termination=True)
            xvd_n.size = int(snapshot_detail.volume_size)  # size in Gigabytes
            xvd_n.volume_type = 'gp2'
            xvd_n.snapshot_id = snapshot_id
            last_char = chr(ord('d') + disks + SCRATCH_DISKS)
            bdm['/dev/xvd' + last_char] = xvd_n
            disks += 1

        user_data_mime = get_mime_encoded_user_data(
            instance_details,
            setup_disks,
            user_data)
        LOGGER.info('{0}'.format(user_data_mime))

        ec2_helper.run_spot_instance(
            ami_id,
            spot_price,
            user_data_mime,
            instance_type,
            None,
            created_by,
            'BIG Clean',
            instance_details=instance_details,
            zone=zone,
            ephemeral=True,
            bdm=bdm)
    else:
        LOGGER.error('Cannot get a spot instance of {0} for ${1}'.format(instance_type, spot_price))


@echo
def check_args(args):
    """
    Check the arguments and prompt for new ones
    """
    map_args = {}

    if args['snapshots'] is None:
        return None

    if args['instance_type'] is None:
        return None

    if args['name'] is None:
        return None

    instance_details = AWS_INSTANCES.get(args['instance_type'])
    if instance_details is None:
        LOGGER.error('The instance type {0} is not supported.'.format(args['instance_type']))
        return None
    else:
        LOGGER.info(
            'instance: {0}, vCPU: {1}, RAM: {2}GB, Disks: {3}x{4}GB, IOPS: {5}'.format(
                args['instance_type'],
                instance_details.vCPU,
                instance_details.memory,
                instance_details.number_disks,
                instance_details.size,
                instance_details.iops_support))

    map_args.update({
        'ami_id': args['ami_id'] if args['ami_id'] is not None else AWS_AMI_ID,
        'created_by': args['created_by'] if args['created_by'] is not None else getpass.getuser(),
        'spot_price': args['spot_price'] if args['spot_price'] is not None else None,
        'user_data': get_script(args['bash_script'] if args['bash_script'] is not None else BASH_SCRIPT_CLEAN_ALL),
        'setup_disks': get_script(BASH_SCRIPT_SETUP_DISKS),
        'instance_details': instance_details,
    })
    return map_args


def main():
    parser = argparse.ArgumentParser('Start a number of CLEAN servers')
    parser.add_argument('-a', '--ami_id', help='the AMI id to use')
    parser.add_argument('-i', '--instance_type', required=True, help='the instance type to use')
    parser.add_argument('-c', '--created_by', help='the username to use')
    parser.add_argument('-n', '--name', required=True, help='the instance name to use')
    parser.add_argument('-s', '--spot_price', type=float, help='the spot price to use')
    parser.add_argument('-b', '--bash_script', help='the bash script to use')
    parser.add_argument('-p', '--processes', type=int, default=1, help='the number of processes to run')
    parser.add_argument('snapshots', nargs='+', help='the snapshots to use')

    args = vars(parser.parse_args())

    corrected_args = check_args(args)
    if corrected_args is None:
        LOGGER.error('The arguments are incorrect: {0}'.format(args))
    else:
        start_server(
            args['processes'],
            corrected_args['ami_id'],
            corrected_args['user_data'],
            corrected_args['setup_disks'],
            args['instance_type'],
            args['snapshots'],
            corrected_args['created_by'],
            args['name'],
            corrected_args['instance_details'],
            corrected_args['spot_price'])

if __name__ == "__main__":
    # -i r3.4xlarge -n "Kevin CLEAN" -s 0.30 snap-e72805f1
    main()
