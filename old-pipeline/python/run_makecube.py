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
Run the makecube task
"""
import argparse
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import getpass

from common import LOGGER, make_safe_filename, get_script, get_cloud_init
from settings_file import AWS_AMI_ID, BASH_SCRIPT_MAKECUBE, AWS_INSTANCES, BASH_SCRIPT_SETUP_DISKS, PIP_PACKAGES
from ec2_helper import EC2Helper


def get_mime_encoded_user_data(data, observation_id, setup_disks, bottom_frequency, frequency_range, swap_size):
    """
    AWS allows for a multipart m
    """
    # Build the mime message
    user_data = MIMEMultipart()
    user_data.attach(get_cloud_init())

    data_formatted = data.format(
        observation_id,
        PIP_PACKAGES,
        bottom_frequency if bottom_frequency is not None else '',
        frequency_range if frequency_range is not None else '',
        swap_size)
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
        spot_price,
        ebs,
        bottom_frequency,
        frequency_range):
    LOGGER.info('obs_id: {0}, bottom_frequency: {1}, frequency_range: {2}'.format(obs_id, bottom_frequency, frequency_range))
    ec2_helper = EC2Helper()
    zone = ec2_helper.get_cheapest_spot_price(instance_type, spot_price)

    if zone is not None:
        # Swap size
        if ebs is None:
            swap_size = 1
        else:
            ephemeral_size = instance_details.number_disks * instance_details.size
            swap_size = min(int(ephemeral_size * 0.75), 16)

        user_data_mime = get_mime_encoded_user_data(
            user_data,
            obs_id,
            setup_disks,
            bottom_frequency,
            frequency_range,
            swap_size
        )
        LOGGER.info('{0}'.format(user_data_mime))

        ec2_helper.run_spot_instance(
            ami_id,
            spot_price,
            user_data_mime,
            instance_type,
            None,
            created_by,
            name + '- {0}'.format(obs_id),
            instance_details=instance_details,
            zone=zone,
            ebs_size=ebs,
            number_ebs_volumes=4,
            ephemeral=True)
    else:
        LOGGER.error('Cannot get a spot instance of {0} for ${1}'.format(instance_type, spot_price))


def check_args(args):
    """
    Check the arguments and prompt for new ones
    """
    map_args = {}

    if args['obs_id'] is None:
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
        'user_data': get_script(args['bash_script'] if args['bash_script'] is not None else BASH_SCRIPT_MAKECUBE),
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
    parser.add_argument('-e', '--ebs', type=int, help='the size in GB of any EBS volume')
    parser.add_argument('bottom_frequency', help='The bottom frequency')
    parser.add_argument('frequency_range', help='the range of frequencies')
    parser.add_argument('obs_id', help='the observation id')

    args = vars(parser.parse_args())

    corrected_args = check_args(args)
    if corrected_args is None:
        LOGGER.error('The arguments are incorrect: {0}'.format(args))
    else:
        start_servers(
            corrected_args['ami_id'],
            corrected_args['user_data'],
            corrected_args['setup_disks'],
            args['instance_type'],
            make_safe_filename(args['obs_id']),
            corrected_args['created_by'],
            args['name'],
            corrected_args['instance_details'],
            corrected_args['spot_price'],
            args['ebs'],
            args['bottom_frequency'],
            args['frequency_range'])

if __name__ == "__main__":
    # --ebs 300 -i r3.2xlarge -n "Kevin ImgConcat" -s 0.20 1240 200 1240~1424
    main()

