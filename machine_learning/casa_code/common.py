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

"""
import contextlib
import os
import subprocess
import threading
import time
from cStringIO import StringIO

from casa_code.casa_logging import CasaLogger

LOG = CasaLogger(__name__)
SYMBOLS = {
    'customary': ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_ext': ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa', 'zetta', 'iotta'),
    'iec': ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'iec_ext': ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi', 'zebi', 'yobi'),
}


class ProgressPercentage:
    def __init__(self, filename, expected_size):
        self._filename = filename
        self._size = float(expected_size)
        self._size_mb = bytes2human(expected_size, '{0:.2f}{1}')
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._percentage = -1

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            if self._size >= 1.0:
                percentage = int((self._seen_so_far / self._size) * 100.0)
                if percentage > self._percentage:
                    LOG.info(
                        '{0}  {1} / {2} ({3}%)'.format(
                            self._filename,
                            bytes2human(self._seen_so_far, '{0:.2f}{1}'),
                            self._size_mb,
                            percentage))
                    self._percentage = percentage
            else:
                LOG.warning('Filename: {0}, size: 0'.format(self._filename))


class OutputStream(threading.Thread):
    def __init__(self):
        super(OutputStream, self).__init__()
        self.done = False
        self.buffer = StringIO()
        self.read, self.write = os.pipe()
        self.reader = os.fdopen(self.read)
        self.start()

    def fileno(self):
        return self.write

    def run(self):
        while not self.done:
            self.buffer.write(self.reader.readline())

        self.reader.close()

    def close(self):
        self.done = True
        os.close(self.write)

    def __enter__(self):
        # Theoretically could be used to set up things not in __init__
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


@contextlib.contextmanager
def stopwatch(message):
    t0 = time.time()
    try:
        yield
    finally:
        t1 = time.time()
        LOG.info('Total elapsed time for {0}: {1:.3f}'.format(message, t1 - t0))


def bytes2human(n, format_string='{0:.1f}{1}', symbols='customary'):
    n = int(n)
    if n < 0:
        raise ValueError("n < 0")

    symbols = SYMBOLS[symbols]
    prefix = {}
    for i, s in enumerate(symbols[1:]):
        prefix[s] = 1 << (i + 1) * 10
    for symbol in reversed(symbols[1:]):
        if n >= prefix[symbol]:
            value = float(n) / prefix[symbol]
            return format_string.format(value, symbol)
    return format_string.format(n, symbols[0])


def run_command(command):
    LOG.info(command)
    with OutputStream() as stream:
        process = subprocess.Popen(command, bufsize=1, shell=True, stdout=stream, stderr=subprocess.STDOUT, env=os.environ.copy())
        while process.poll() is None:
            time.sleep(1)

    output = stream.buffer.getvalue()
    LOG.info('{0}, output follows.\n{1}'.format(command, output))

    return process.returncode
