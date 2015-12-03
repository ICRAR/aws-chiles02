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
import multiprocessing
from string import find
import sys

from common import get_script, get_cloud_init, Consumer, LOGGER
from echo import echo
from settings_file import AWS_AMI_ID, BASH_SCRIPT_CLEAN_02, BASH_SCRIPT_SETUP_DISKS, AWS_INSTANCES, PIP_PACKAGES
from ec2_helper import EC2Helper


LOGGER.info('PYTHONPATH = {0}'.format(sys.path))


class Task(object):
    """
    The actual task
    """
    def __init__(
            self,
            ami_id,
            user_data,
            setup_disks,
            instance_type,
            frequency_id,
            created_by,
            name,
            spot_price,
            instance_details):
        self._ami_id = ami_id
        self._user_data = user_data
        self._setup_disks = setup_disks
        self._instance_type = instance_type
        self._frequency_id = frequency_id
        self._created_by = created_by
        self._name = name
        self._spot_price = spot_price
        self._instance_details = instance_details

    def __call__(self):
        """
        Actually run the job
        """
        LOGGER.info('frequency_id: {0}'.format(self._frequency_id))
        ec2_helper = EC2Helper()
        zone = ec2_helper.get_cheapest_spot_price(self._instance_type, self._spot_price)

        if zone is not None:
            user_data_mime = self.get_mime_encoded_user_data()
            LOGGER.info('{0}'.format(user_data_mime))

            ec2_helper.run_spot_instance(
                self._ami_id,
                self._spot_price,
                user_data_mime,
                self._instance_type, None,
                self._created_by,
                '{0}-{1}'.format(self._frequency_id, self._name),
                instance_details=self._instance_details,
                zone=zone,
                ephemeral=True)
        else:
            LOGGER.error('Cannot get a spot instance of {0} for ${1}'.format(self._instance_type, self._spot_price))

    def get_mime_encoded_user_data(self):
        """
        AWS allows for a multipart m
        """
        # Split the frequencies
        index_underscore = find(self._frequency_id, '_')
        index_tilde = find(self._frequency_id, '~')
        min_freq = self._frequency_id[index_underscore + 1:index_tilde]
        max_freq = self._frequency_id[index_tilde + 1:]
        LOGGER.info('min_freq: {0}, max_freq: {1}'.format(min_freq, max_freq))

        # Build the mime message
        user_data = MIMEMultipart()
        user_data.attach(get_cloud_init())

        swap_size = self.get_swap_size()
        data_formatted = self._user_data.format(self._frequency_id, min_freq, max_freq, swap_size, PIP_PACKAGES)
        user_data.attach(MIMEText(self._setup_disks + data_formatted))
        return user_data.as_string()

    def get_swap_size(self):
        ephemeral_size = self._instance_details.number_disks * self._instance_details.size
        if ephemeral_size > 100:
            return 8
        elif ephemeral_size > 32:
            return 2
        else:
            return 1


def start_servers(
        processes,
        ami_id,
        user_data,
        setup_disks,
        instance_type,
        frequency_ids,
        created_by,
        name,
        instance_details,
        spot_price):
    # Create the queue
    tasks = multiprocessing.JoinableQueue()

    # Start the consumers
    for x in range(processes):
        consumer = Consumer(tasks)
        consumer.start()

    for frequency_id in frequency_ids:
        tasks.put(
            Task(
                ami_id,
                user_data,
                setup_disks,
                instance_type,
                frequency_id,
                created_by,
                name,
                spot_price,
                instance_details))

        # Add a poison pill to shut things down
    for x in range(processes):
        tasks.put(None)

    # Wait for the queue to terminate
    tasks.join()


@echo
def check_args(args):
    """
    Check the arguments and prompt for new ones
    """
    map_args = {}

    if args['frequencies'] is None:
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
        'user_data': get_script(args['bash_script'] if args['bash_script'] is not None else BASH_SCRIPT_CLEAN_02),
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
    parser.add_argument('frequencies', nargs='+', help='the frequencies to use (vis_14XX~14YY')

    args = vars(parser.parse_args())

    corrected_args = check_args(args)
    if corrected_args is None:
        LOGGER.error('The arguments are incorrect: {0}'.format(args))
    else:
        start_servers(
            args['processes'],
            corrected_args['ami_id'],
            corrected_args['user_data'],
            corrected_args['setup_disks'],
            args['instance_type'],
            args['frequencies'],
            corrected_args['created_by'],
            args['name'],
            corrected_args['instance_details'],
            corrected_args['spot_price'])

if __name__ == "__main__":
    # -i r3.4xlarge -n "Kevin CLEAN" -s 0.30 vis_1400~1404
    main()
