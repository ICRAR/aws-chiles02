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
Common code
"""
import logging
import multiprocessing
import os
import re
import tarfile
import unicodedata
from contextlib import closing
from email.mime.text import MIMEText
from os.path import join, dirname, expanduser, getsize

from echo import echo


def get_logger(level=multiprocessing.SUBDEBUG):
    logger = multiprocessing.get_logger()
    formatter = logging.Formatter('[%(processName)s]:%(asctime)-15s:%(levelname)s:%(module)s:%(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = 0

    if level:
        logger.setLevel(level)

    return logger


LOGGER = get_logger()


class Consumer(multiprocessing.Process):
    """
    A class to process jobs from the queue
    """
    def __init__(self, queue):
        multiprocessing.Process.__init__(self)
        self._queue = queue

    def run(self):
        """
        Sit in a loop
        """
        while True:
            LOGGER.info('Getting a task')
            next_task = self._queue.get()
            if next_task is None:
                # Poison pill means shutdown this consumer
                LOGGER.info('Exiting consumer')
                self._queue.task_done()
                return
            LOGGER.info('Executing the task')
            # noinspection PyBroadException
            try:
                next_task()
            except:
                LOGGER.exception('Exception in consumer')
            finally:
                self._queue.task_done()


def make_safe_filename(name):
    if isinstance(name, unicode):
        name = unicodedata.normalize('NFKD', name)
        name = name.encode('ascii', 'ignore').lower()
    else:
        name = name.lower()
    name = re.sub(r'[^a-z0-9]+', '-', name).strip('-')
    name = re.sub(r'[-]+', '-', name)

    return name


def get_boto_data():
    dot_boto = join(expanduser('~'), '.boto_chiles')
    with open(dot_boto, 'r') as my_file:
        data = my_file.read()

    return data


def get_script(file_name):
    """
    Get the script from the bash directory
    """
    here = dirname(__file__)
    bash = join(here, '../bash', file_name)
    with open(bash, 'r') as my_file:
        data = my_file.read()

    return data


def yaml_text(input_text):
    """
    Yaml's the text
    """
    list_lines = []
    for line in input_text.split('\n'):
        list_lines.append('      ' + line)
    return '\n'.join(list_lines)


def get_cloud_init():
    return MIMEText('''#cloud-config
repo_update: true
repo_upgrade: all

# Install additional packages on first boot
packages:
 - wget
 - git
 - libXrandr
 - libXfixes
 - libXcursor
 - libXinerama
 - htop
 - sysstat
 - iotop

# Add a kill command so if it goes TU we will kill the instance
#power_state:
# delay: "+1440"
# mode: halt
# message: Kill command executed
# timeout: 120

# Write the boto file
write_files:
 - content: |
{0}
   path: /etc/boto.cfg

# Log all cloud-init process output (info & errors) to a logfile
output : {{ all : ">> /var/log/chiles-output.log" }}

# Final_message written to log when cloud-init processes are finished
final_message: "System boot (via cloud-init) is COMPLETE, after $UPTIME seconds. Finished at $TIMESTAMP"
'''.format(yaml_text(get_boto_data())))


@echo
def make_tarfile(output_filename, source_dir):
    LOGGER.info('output_filename: {0}, source_dir: {1}'.format(output_filename, source_dir))
    with closing(tarfile.open(output_filename, 'w:')) as tar:
        for entry in os.listdir(source_dir):
            full_filename = join(source_dir, entry)
            tar.add(full_filename, arcname=entry)


@echo
def can_be_multipart_tar(directory_to_check):
    total_size = 0
    for root, dir_names, filenames in os.walk(directory_to_check):
        for file_name in filenames:
            full_path = join(root, file_name)
            total_size += getsize(full_path)

    return total_size > 100 * 1024 * 1024
