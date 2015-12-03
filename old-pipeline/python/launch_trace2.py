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
    https://www.kernel.org/doc/Documentation/filesystems/proc.txt

    /proc/stat fields specification
    The very first  "cpu" line aggregates the  numbers in all  of the other "cpuN"
    lines.  These numbers identify the amount of time the CPU has spent performing
    different kinds of work.  Time units are in USER_HZ (typically hundredths of a
    second).  The meanings of the columns are as follows, from left to right:

    - user: normal processes executing in user mode
    - nice: niced processes executing in user mode
    - system: processes executing in kernel mode
    - idle: twiddling thumbs
    - iowait: waiting for I/O to complete
    - irq: servicing interrupts
    - softirq: servicing softirqs
    - steal: involuntary wait
    - guest: running a normal guest
    - guest_nice: running a niced guest

    /proc/PID/stat fields specification
    Field          Content
      pid           process id
      tcomm         filename of the executable
      state         state (R is running, S is sleeping, D is sleeping in an
                    uninterruptible wait, Z is zombie, T is traced or stopped)
      ppid          process id of the parent process
      pgrp          pgrp of the process
      sid           session id
      tty_nr        tty the process uses
      tty_pgrp      pgrp of the tty
      flags         task flags
      min_flt       number of minor faults
      cmin_flt      number of minor faults with child's
      maj_flt       number of major faults
      cmaj_flt      number of major faults with child's
      utime         user mode jiffies
      stime         kernel mode jiffies
      cutime        user mode jiffies with child's
      cstime        kernel mode jiffies with child's
      priority      priority level
      nice          nice level
      num_threads   number of threads
      it_real_value    (obsolete, always 0)
      start_time    time the process started after system boot
      vsize         virtual memory size
      rss           resident set memory size
      rsslim        current limit in bytes on the rss
      start_code    address above which program text can run
      end_code      address below which program text can run
      start_stack   address of the start of the main process stack
      esp           current value of ESP
      eip           current value of EIP
      pending       bitmap of pending signals
      blocked       bitmap of blocked signals
      sigign        bitmap of ignored signals
      sigcatch      bitmap of caught signals
      wchan         address where process went to sleep
      0             (place holder)
      0             (place holder)
      exit_signal   signal to send to parent thread on exit
      task_cpu      which CPU the task is scheduled on
      rt_priority   realtime priority
      policy        scheduling policy (man sched_setscheduler)
      blkio_ticks   time spent waiting for block IO
      gtime         guest time of the task in jiffies
      cgtime        guest time of the task children in jiffies
      start_data    address above which program data+bss is placed
      end_data      address below which program data+bss is placed
      start_brk     address above which program heap can be expanded with brk()
      arg_start     address above which program command line is placed
      arg_end       address below which program command line is placed
      env_start     address above which program environment is placed
      env_end       address below which program environment is placed
      exit_code     the thread's exit_code in the form reported by the waitpid system call

/proc/{PID}/io - accessable only by root
Description
-----------

rchar
-----

I/O counter: chars read
The number of bytes which this task has caused to be read from storage. This
is simply the sum of bytes which this process passed to read() and pread().
It includes things like tty IO and it is unaffected by whether or not actual
physical disk IO was required (the read might have been satisfied from
pagecache)


wchar
-----

I/O counter: chars written
The number of bytes which this task has caused, or shall cause to be written
to disk. Similar caveats apply here as with rchar.


syscr
-----

I/O counter: read syscalls
Attempt to count the number of read I/O operations, i.e. syscalls like read()
and pread().


syscw
-----

I/O counter: write syscalls
Attempt to count the number of write I/O operations, i.e. syscalls like
write() and pwrite().


read_bytes
----------

I/O counter: bytes read
Attempt to count the number of bytes which this process really did cause to
be fetched from the storage layer. Done at the submit_bio() level, so it is
accurate for block-backed filesystems. <please add status regarding NFS and
CIFS at a later time>


write_bytes
-----------

I/O counter: bytes written
Attempt to count the number of bytes which this process caused to be sent to
the storage layer. This is done at page-dirtying time.


cancelled_write_bytes
---------------------

The big inaccuracy here is truncate. If a process writes 1MB to a file and
then deletes the file, it will in fact perform no writeout. But it will have
been accounted as having caused 1MB of write.
In other words: The number of bytes which this process caused to not happen,
by truncating pagecache. A task can cause "negative" IO too. If this task
truncates some dirty pagecache, some IO which another task has been accounted
for (in its write_bytes) will not be happening. We _could_ just subtract that
from the truncating task's write_bytes, but there is information loss in doing
that.

"""
import csv
import getpass
import logging
import os
from os import makedirs
import subprocess
import sys
from os.path import exists, join
import time
from datetime import datetime
import resource

from psutil import Process, cpu_count


TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

I_STATE = 2
I_UTIME = 13
I_STIME = 14
I_CUTIME = 15
I_CSTIME = 16
I_PRIORITY = 17
I_NICE = 18
I_NUM_THREADS = 19
I_VSIZE = 22
I_RSS = 23
I_BLKIO_TICKS = 41

BUFFER_SIZE_10K = 10 * 1024
BUFFER_SIZE_100K = 100 * 1024
TRACE_DETAILS = 'trace_details'
STAT_DETAILS = 'stat_details'
PROCESS_DETAILS = 'process_details'
LOG_DETAILS = 'log_details'
FSTAT = '/proc/stat'
EPOCH = datetime(1970, 1, 1)
LOGS_DIR = '/tmp/trace_logs'
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)


class Trace():
    def __init__(self, command_list, sample_rate=1):

        self._command_list = command_list
        self._user = getpass.getuser()
        self._sample_rate = sample_rate
        self._set_pids = set()
        self._date_string = None
        self._start_time = None
        self._timestamp = None
        self._csv_stat_writer = None
        self._csv_process_writer = None
        self._csv_log_writer = None
        if self._user == 'root':
            LOG.info('Running as: {0}'.format(self._user))
        else:
            LOG.info('Running as: {0}. IO stats will not be available unless you run as root'.format(self._user))

        # Create the logs directory
        LOG.info("Checking for the logs directory {0}".format(LOGS_DIR))
        if not exists(LOGS_DIR):
            LOG.info("Creating the logs directory {0}".format(LOGS_DIR))
            makedirs(LOGS_DIR)

    def _get_samples(self, list_processes):
        self._timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)

        # Read the data from /proc/stat for the system
        with open(FSTAT, 'r') as file_stat:
            first_line = file_stat.readline()
        elements = first_line.split()

        # Now do the individual processes
        self._csv_stat_writer.writerow(
            [self._timestamp,
             elements[1],
             elements[2],
             elements[3],
             elements[4],
             elements[5],
             elements[6],
             elements[7],
             elements[8],
             elements[9],
             elements[10]]
        )
        for process in list_processes:
            # noinspection PyBroadException
            try:
                pid = process.pid
                #
                if pid not in self._set_pids:
                    row = [pid,
                           process.ppid(),
                           process.name(),
                           ' '.join(process.cmdline()),
                           datetime.fromtimestamp(process.create_time()).strftime(TIMESTAMP_FORMAT)]
                    self._csv_process_writer.writerow(row)
                    self._set_pids.add(pid)
                self._collect_sample(pid)
            except Exception:
                LOG.exception('_get_samples: Pid {0} no longer running'.format(process.pid))

    def _collect_sample(self, pid):
        # Catch the process stopping whilst we are sampling
        # noinspection PyBroadException
        try:
            file_name1 = "/proc/{0}/stat".format(pid)
            with open(file_name1) as f:
                line_stat = f.readline()

            if self._user == 'root':
                file_name2 = "/proc/{0}/io".format(pid)
                with open(file_name2) as f:
                    lines_io = f.readlines()
            else:
                lines_io = '''rchar: 0
wchar: 0
syscr: 0
syscw: 0
read_bytes: 0
write_bytes: 0
cancelled_write_bytes: 0'''

        except Exception:
            LOG.exception('_collect_sample: Pid {0} no longer running'.format(pid))
            return

        stat_details = line_stat.split()
        io_details = []
        for line in lines_io:
            if len(line) > 0:
                io_details.append(line.split()[1])

        if len(stat_details) < I_BLKIO_TICKS or len(io_details) < 6:
            return

        self._csv_log_writer.writerow(
            [pid,
             self._timestamp,
             stat_details[I_STATE],
             stat_details[I_UTIME],
             stat_details[I_STIME],
             stat_details[I_CUTIME],
             stat_details[I_CSTIME],
             stat_details[I_PRIORITY],
             stat_details[I_NICE],
             stat_details[I_NUM_THREADS],
             stat_details[I_VSIZE],
             stat_details[I_RSS],
             stat_details[I_BLKIO_TICKS],
             io_details[0],
             io_details[1],
             io_details[2],
             io_details[3],
             io_details[4],
             io_details[5],
             io_details[6]]
        )

    def _get_file_name(self, pid, log_type):
        return join(LOGS_DIR, '{0}_{1}_{2}.csv'.format(self._date_string, pid, log_type))

    def run(self):
        # Get the start time
        start_time = datetime.now()
        self._date_string = start_time.strftime('%Y%m%d%H%M%S')
        timestamp = start_time.strftime(TIMESTAMP_FORMAT)

        # Spin up the main process
        sp = subprocess.Popen(self._command_list)

        # Open trace file
        trace_details_file_name = self._get_file_name(sp.pid, TRACE_DETAILS)
        with open(trace_details_file_name, 'w', 1) as trace_file:
            writer = csv.writer(trace_file, lineterminator='\n')
            writer.writerow(['start_time', 'cmd_line', 'sample_rate', 'tick', 'page_size', 'cpu_count'])
            writer.writerow([timestamp,
                             ' '.join(self._command_list),
                             self._sample_rate,
                             os.sysconf(os.sysconf_names['SC_CLK_TCK']),
                             resource.getpagesize(),
                             cpu_count()])

        stat_file = open(self._get_file_name(sp.pid, STAT_DETAILS), 'w', BUFFER_SIZE_10K)
        self._csv_stat_writer = csv.writer(stat_file, lineterminator='\n')
        self._csv_stat_writer.writerow(
            ['timestamp',
             'user',
             'nice',
             'system',
             'idle',
             'iowait',
             'irq',
             'softirq',
             'steal',
             'guest',
             'guest_nice']
        )
        process_file = open(self._get_file_name(sp.pid, PROCESS_DETAILS), 'w', BUFFER_SIZE_10K)
        self._csv_process_writer = csv.writer(process_file, lineterminator='\n')
        self._csv_process_writer.writerow(
            ['pid',
             'ppid',
             'name',
             'cmd_line',
             'create_time']
        )
        log_file = open(self._get_file_name(sp.pid, LOG_DETAILS), 'w', BUFFER_SIZE_100K)
        self._csv_log_writer = csv.writer(log_file, lineterminator='\n')
        self._csv_log_writer.writerow(
            ['pid',
             'timestamp',
             'state',
             'utime',
             'stime',
             'cutime',
             'cstime',
             'priority',
             'nice',
             'num_threads',
             'vsize',
             'rss',
             'blkio_ticks',
             'rchar',
             'wchar',
             'syscr',
             'syscw',
             'read_bytes',
             'write_bytes',
             'cancelled_write_bytes']
        )

        # noinspection PyBroadException
        try:
            main_process = Process(sp.pid)
            while sp.poll() is None:
                now = time.time()

                pids = [Process(sp.pid)]
                pids.extend(main_process.children(recursive=True))
                self._get_samples(pids)

                time.sleep(max(1 - (time.time() - now), 0.001))
        except Exception:
            LOG.exception('An exception slipped through')
        finally:
            # Close the writers
            stat_file.close()
            process_file.close()
            log_file.close()


def usage():
    LOG.info('python launch_trace.py app')
    LOG.info('e.g. python launch_trace.py ls -l')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)

    trace = Trace(sys.argv[1:])
    trace.run()
