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
import logging
from os import makedirs
import subprocess
import sys
from os.path import exists, join
import time
import datetime
from psutil import Process
from sqlalchemy import create_engine
from database import TRACE_METADATA, PROCESS_DETAILS
from trace_cpu_mem import collect_sample, process_sample, compute_usage

MAP_SAMPLES = {}
MAP_PROCESS_DETAILS = {}
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)


def usage():
    LOG.info('python launch_trace.py app')
    LOG.info('e.g. python launch_trace.py ls -l')


def get_samples(list_processes):
    for process in list_processes:
        pid = process.pid
        samples = MAP_SAMPLES.get(pid)
        if samples is None:
            samples = []
            MAP_SAMPLES[pid] = samples
            MAP_PROCESS_DETAILS[pid] = [
                pid,
                process.ppid(),
                process.name(),
                process.cmdline(),
                process.create_time()
            ]

        samples.append(collect_sample(pid))


def trace():
    start_time = datetime.datetime.now()

    # Spin up the main process
    cmd_list = sys.argv[1:]
    sp = subprocess.Popen(cmd_list)
    main_process = Process(sp.pid)
    while sp.poll() is None:
        now = time.time()

        pids = [Process(sp.pid)]
        pids.extend(main_process.children(recursive=True))
        get_samples(pids)

        time.sleep(max(1 - (time.time() - now), 0.001))

    logs_dir = '/tmp/trace_logs'
    LOG.info("Checking for the logs directory {0}".format(logs_dir))
    if not exists(logs_dir):
        LOG.info("Creating the logs directory {0}".format(logs_dir))
        makedirs(logs_dir)

    sqlite_file = join(logs_dir, '{0}_{1}_log.db'.format(start_time.strftime('%Y%m%d%H%M%S'), sp.pid))
    engine = create_engine('sqlite:///{0}'.format(sqlite_file))
    sqlite_connection = engine.connect()
    TRACE_METADATA.create_all(sqlite_connection)
    LOG.info("Processing samples ...")

    transaction = sqlite_connection.begin()
    insert = PROCESS_DETAILS.insert()
    for key in MAP_PROCESS_DETAILS.keys():
        row = MAP_PROCESS_DETAILS[key]
        sqlite_connection.execute(
            insert,
            pid=row[0],
            ppid=row[1],
            name=row[2],
            cmd_line=' '.join(row[3]),
            create_time=row[4],
        )
    transaction.commit()

    for key in MAP_SAMPLES.keys():
        LOG.info('Writing data for {0}'.format(key))
        pas = [process_sample(x) for x in MAP_SAMPLES.get(key)]
        LOG.info("Compute CPU statistics ...")
        compute_usage(pas, key, print_list=False, sqlite=sqlite_connection)

    sqlite_connection.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)
    trace()
