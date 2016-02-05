#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia
#    Copyright by UWA (in the framework of the ICRAR)
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
Common code to
"""
import getpass
import os
import subprocess
import time
import uuid
from Queue import Queue, Empty
from threading import Thread

FREQUENCY_WIDTH = 4
FREQUENCY_GROUPS = []
COUNTERS = {}
INPUT_MS_SUFFIX = '_calibrated_deepfield.ms'

CONTAINER_JAVA_S3_COPY = 'sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/java-s3-copy:latest'
CONTAINER_CHILES02 = 'sdp-docker-registry.icrar.uwa.edu.au:8080/kevin/chiles02:latest'

# for bottom_freq in range(1200, 1204, FREQUENCY_WIDTH):
for bottom_freq in range(940, 1424, FREQUENCY_WIDTH):
    FREQUENCY_GROUPS.append([bottom_freq, bottom_freq + FREQUENCY_WIDTH])


def make_groups_of_frequencies(group_size):
    groups = []
    count = 0
    batch = []
    for frequency_group in FREQUENCY_GROUPS:
        batch.append(frequency_group)

        count += 1
        if count == group_size:
            groups.append(batch)
            batch = []
            count = 0

    if len(batch) > 0:
        groups.append(batch)

    return groups


def get_oid(count_type):
    count = COUNTERS.get(count_type)
    if count is None:
        count = 1
    else:
        count += 1
    COUNTERS[count_type] = count

    return '{0}__{1:06d}'.format(count_type, count)


def get_uid():
    return str(uuid.uuid4())


def get_module_name(item):
    return item.__module__ + '.' + item.__name__


def get_session_id():
    return '{0}-{1}'.format(
            getpass.getuser(),
            int(time.time())
    )


def get_observation(s3_path):
    if s3_path.endswith('.tar'):
        s3_path = s3_path[:-4]
    elements = s3_path[:-len(INPUT_MS_SUFFIX)]
    return elements


def run_command(command):
    io_queue = Queue()

    def stream_watcher(identifier, stream):
        for line in stream:
            io_queue.put((identifier, line))

        if not stream.closed:
            stream.close()

    process = subprocess.Popen(command, bufsize=1, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ.copy())

    Thread(target=stream_watcher,
           name='stdout-watcher',
           args=('STDOUT', process.stdout)
           ).start()
    Thread(target=stream_watcher,
           name='stderr-watcher',
           args=('STDERR', process.stderr)
           ).start()

    def printer():
        while True:
            try:
                # Block for 2 seconds.
                item = io_queue.get(True, 2)
            except Empty:
                # No output in either streams for two seconds. Are we done?
                if process.poll() is not None:
                    break
            else:
                identifier, line = item
                print line.rstrip()

    Thread(target=printer,
           name='printer'
           ).start()
    return_code = process.poll()
    return return_code
