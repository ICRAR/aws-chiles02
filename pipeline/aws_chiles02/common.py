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
import Queue
import getpass
import os
import subprocess
import threading
import time
import uuid

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


def split_s3_url(s3_url):
    """
    Split the s3 url into bucket and key
    :param s3_url:
    :return:

    >>> split_s3_url('s3://bucket_name/key/morekey/ms.tar')
    ('bucket_name', 'key/morekey/ms.tar')
    """
    body = s3_url[5:]
    index = body.find('/')
    return body[:index], body[index+1:]


def get_observation(s3_path):
    if s3_path.endswith('.tar'):
        s3_path = s3_path[:-4]
    elements = s3_path[:-len(INPUT_MS_SUFFIX)]
    return elements


class AsynchronousFileReader(threading.Thread):
    """
    Helper class to implement asynchronous reading of a file
    in a separate thread. Pushes read lines on a queue to
    be consumed in another thread.
    """
    def __init__(self, fd, queue):
        assert isinstance(queue, Queue.Queue)
        assert callable(fd.readline)
        threading.Thread.__init__(self)
        self._fd = fd
        self._queue = queue

    def run(self):
        """
        The body of the tread: read lines and put them on the queue.
        """
        for line in iter(self._fd.readline, ''):
            self._queue.put(line)

    def eof(self):
        """
        Check whether there is no more content to expect.
        """
        return not self.is_alive() and self._queue.empty()


def run_command(command):

    process = subprocess.Popen(command, bufsize=1, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ.copy())

    # Launch the asynchronous readers of the processes stdout and stderr.
    stdout_queue = Queue.Queue()
    stdout_reader = AsynchronousFileReader(process.stdout, stdout_queue)
    stdout_reader.start()
    stderr_queue = Queue.Queue()
    stderr_reader = AsynchronousFileReader(process.stderr, stderr_queue)
    stderr_reader.start()

    # Check the queues if we received some output (until there is nothing more to get).
    while not stdout_reader.eof() or not stderr_reader.eof():
        # Show what we received from standard output.
        while not stdout_queue.empty():
            line = stdout_queue.get()
            print line.rstrip()

        # Show what we received from standard error.
        while not stderr_queue.empty():
            line = stderr_queue.get()
            print line.rstrip()

        # Sleep a bit before asking the readers again.
        time.sleep(2)

    # Let's be tidy and join the threads we've started.
    stdout_reader.join()
    stderr_reader.join()

    # Close subprocess' file descriptors.
    process.stdout.close()
    process.stderr.close()

    return_code = process.poll()
    return return_code
