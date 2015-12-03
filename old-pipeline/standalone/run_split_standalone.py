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
Start a number of CVEL servers
"""
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import sys

from common import get_cloud_init, get_script, LOGGER
from settings_file import OBS_IDS, AWS_INSTANCES, BASH_SCRIPT_SETUP_DISKS, PIP_PACKAGES
from ec2_helper import EC2Helper
import datetime


LOGGER.info('PYTHONPATH = {0}'.format(sys.path))


def get_mime_encoded_user_data(volume_id, setup_disks, in_user_data, now):
    """
    AWS allows for a multipart m
    """
    user_data = MIMEMultipart()
    user_data.attach(get_cloud_init())

    data_formatted = in_user_data.format(volume_id, now, PIP_PACKAGES)
    LOGGER.info(data_formatted)
    user_data.attach(MIMEText(setup_disks + data_formatted))
    return user_data.as_string()


def start_servers(
        ami_id,
        user_data,
        setup_disks,
        instance_type,
        obs_id,
        created_by,
        name,
        instance_details,
        spot_price):

    snapshot_id = OBS_IDS.get(obs_id)
    if snapshot_id is None:
        LOGGER.warning('The obs-id: {0} does not exist in the settings file')
    else:
        ec2_helper = EC2Helper()
        iops = None
        if instance_details.iops_support:
            iops = 500

        zone = ec2_helper.get_cheapest_spot_price(instance_type, spot_price)
        if zone is not None:
            volume, snapshot_name = ec2_helper.create_volume(snapshot_id, zone, iops=iops)
            LOGGER.info('obs_id: {0}, volume_name: {1}'.format(obs_id, snapshot_name))
            now = datetime.datetime.now()
            user_data_mime = get_mime_encoded_user_data(volume.id, setup_disks, user_data, now.strftime('%Y-%m-%dT%H-%M-%S'))

            if spot_price is not None:
                ec2_helper.run_spot_instance(
                    ami_id,
                    spot_price,
                    user_data_mime,
                    instance_type,
                    volume.id,
                    created_by,
                    '{1}-{0}'.format(name, snapshot_name),
                    instance_details,
                    zone,
                    ephemeral=True)
        else:
            LOGGER.error('Cannot get a spot instance of {0} for ${1}'.format(instance_type, spot_price))


def main():
    start_servers(
        'ami-3f0e7d05',
        #get_script('run_split_standalone.bash'),
        get_script('run_split_standalone_strace.bash'),
        get_script(BASH_SCRIPT_SETUP_DISKS),
        'r3.xlarge',
        '20131122_941_6',
        'Kevin',
        'Split Standalone',
        AWS_INSTANCES.get('r3.xlarge'),
        '0.10')

if __name__ == "__main__":
    # -i r3.xlarge -n "Kevin cvel test" -s 0.10 20131025_951_4 20131031_951_4
    main()
