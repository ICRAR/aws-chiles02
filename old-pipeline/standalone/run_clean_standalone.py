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
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import multiprocessing
from string import find
import sys

from common import get_script, get_cloud_init, Consumer, LOGGER
from settings_file import BASH_SCRIPT_SETUP_DISKS, AWS_INSTANCES, PIP_PACKAGES
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
        data_formatted = self._user_data.format(self._frequency_id, swap_size, PIP_PACKAGES)
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


def main():
    start_servers(
        1,
        'ami-3f0e7d05',
        #get_script('run_clean_standalone.bash'),
        get_script('run_clean_standalone_strace.bash'),
        get_script(BASH_SCRIPT_SETUP_DISKS),
        'r3.4xlarge',
        ['vis_1136~1140'],
        'Kevin',
        'Standalone Clean',
        AWS_INSTANCES.get('r3.4xlarge'),
        '2.00')

if __name__ == "__main__":
    main()
