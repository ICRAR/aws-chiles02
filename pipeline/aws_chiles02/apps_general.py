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
My Docker Apps
"""
import logging
import os
import shutil
from time import sleep

import boto3
import jsonpickle
import psutil
import six
from boto3.s3.transfer import S3Transfer
from dlg.drop import BarrierAppDROP, DirectoryContainer, FileDROP

from aws_chiles02.build_readme import build_file
from aws_chiles02.common import ProgressPercentage, run_command
from aws_chiles02.settings_file import AWS_REGION

LOG = logging.getLogger(__name__)
LOG.info("Python 2: {}, Python 3: {}".format(six.PY2, six.PY3))
DEFAULT_STORAGE = "INTELLIGENT_TIERING"
COPY_TO_GLACIER = {"move_to_glacier": True}


def tag_s3_object(s3_object, s3_tags):
    if s3_tags is not None and isinstance(s3_tags, dict):
        for key, value in s3_tags.items():
            s3_object.put(Tagging="{}={}".format(key, value))


class ErrorHandling(object):
    def __init__(self):
        self._session_id = None
        self._error_message = None

    def send_error_message(
        self,
        message_text,
        oid,
        uid,
        queue="dlg-messages",
        region=AWS_REGION,
        profile_name="aws-chiles02",
    ):
        self._error_message = message_text
        session = boto3.Session(profile_name=profile_name)
        sqs = session.resource("sqs", region_name=region)
        queue = sqs.get_queue_by_name(QueueName=queue)
        message = {
            "session_id": self._session_id,
            "oid": oid,
            "uid": uid,
            "message": message_text,
        }
        json_message = jsonpickle.encode(message)
        queue.send_message(MessageBody=json_message)

    @property
    def error_message(self):
        return self._error_message

    @property
    def session_id(self):
        return self._session_id


class CopyParameters(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **keywords):
        self._parameter_data = None
        super(CopyParameters, self).__init__(oid, uid, **keywords)

    def initialize(self, **keywords):
        super(CopyParameters, self).initialize(**keywords)
        # The data is in a string so we need to load it to write it
        self._parameter_data = self._getArg(keywords, "parameter_data", None)

    def dataURL(self):
        return type(self).__name__

    def run(self):
        LOG.info("parameter_data: {0}".format(self._parameter_data))
        parameter_file = "/tmp/parameter_data.yaml"
        with open(parameter_file, "w") as yaml_file:
            yaml_file.write(self._parameter_data)

        s3_output = self.outputs[0]
        bucket_name = s3_output.bucket
        key = s3_output.key
        LOG.info("bucket: {0}, key: {1}".format(bucket_name, key))

        session = boto3.Session(profile_name="aws-chiles02")
        s3 = session.resource("s3", use_ssl=False)

        s3_client = s3.meta.client
        transfer = S3Transfer(s3_client)
        transfer.upload_file(
            parameter_file,
            bucket_name,
            key,
            callback=ProgressPercentage(key, float(os.path.getsize(parameter_file))),
            extra_args={"StorageClass": s3_output.storage_class},
        )

        tag_s3_object(s3_client.get_object(Bucket=bucket_name, Key=key), s3_output.tags)


class BuildReadme(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **keywords):
        self._bucket_name = None
        super(BuildReadme, self).__init__(oid, uid, **keywords)

    def initialize(self, **keywords):
        super(BuildReadme, self).initialize(**keywords)
        # The data is in a string so we need to load it to write it
        self._bucket_name = self._getArg(keywords, "bucket_name", None)

    def dataURL(self):
        return type(self).__name__

    def run(self):
        LOG.info("bucket_name: {0}".format(self._bucket_name))
        build_file(self._bucket_name)


class CopyLogFilesApp(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **keywords):
        super(CopyLogFilesApp, self).__init__(oid, uid, **keywords)

    def initialize(self, **keywords):
        super(CopyLogFilesApp, self).initialize(**keywords)
        self._session_id = self._getArg(keywords, "session_id", None)

    def dataURL(self):
        return type(self).__name__

    def run(self):
        log_file_dir = (
            "/mnt/daliuge/dlg_root"
            if os.path.exists("/mnt/daliuge/dlg_root")
            else "/tmp"
        )
        s3_output = self.outputs[0]
        bucket_name = s3_output.bucket
        key = s3_output.key
        LOG.info(
            "dir: {2}, bucket: {0}, key: {1}".format(bucket_name, key, log_file_dir)
        )

        # Make the tar file
        tar_filename = os.path.join(log_file_dir, "log.tar")
        os.chdir(log_file_dir)
        bash = "tar -cvf {} --directory={} d*.log /var/log/cloud-init*.log".format(
            tar_filename, log_file_dir
        )
        return_code = run_command(bash)
        path_exists = os.path.exists(tar_filename)
        if return_code != 0 or not path_exists:
            message = "tar return_code: {0}, exists: {1}".format(
                return_code, path_exists
            )
            LOG.error(message)
            self.send_error_message(message, self.oid, self.uid)
            return return_code

        session = boto3.Session(profile_name="aws-chiles02")
        s3 = session.resource("s3", use_ssl=False)

        s3_client = s3.meta.client
        transfer = S3Transfer(s3_client)
        transfer.upload_file(
            tar_filename,
            bucket_name,
            key,
            callback=ProgressPercentage(key, float(os.path.getsize(tar_filename))),
            extra_args={"StorageClass": s3_output.storage_class},
        )
        tag_s3_object(s3_client.get_object(Bucket=bucket_name, Key=key), s3_output.tags)


class CleanupDirectories(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **keywords):
        self._dry_run = None
        super(CleanupDirectories, self).__init__(oid, uid, **keywords)

    def initialize(self, **keywords):
        super(CleanupDirectories, self).initialize(**keywords)
        self._session_id = self._getArg(keywords, "session_id", None)
        self._dry_run = self._getArg(keywords, "dry_run", None)

    def dataURL(self):
        return type(self).__name__

    def run(self):
        input_files = [
            i.path for i in self.inputs if isinstance(i, (FileDROP, DirectoryContainer))
        ]
        LOG.info("input_files: {0}".format(input_files))
        for input_file in input_files:
            LOG.debug("Looking at {0}".format(input_file))
            if os.path.exists(input_file):
                if os.path.isdir(input_file):
                    LOG.info("Removing directory {0}".format(input_file))

                    def rmtree_onerror(func, path, exc_info):
                        error_message = "onerror(func={0}, path={1}, exc_info={2})".format(
                            func, path, exc_info
                        )
                        LOG.error(error_message)
                        self.send_error_message(error_message, self.oid, self.uid)

                    if self._dry_run:
                        LOG.debug("dry_run = True")
                    else:
                        shutil.rmtree(input_file, onerror=rmtree_onerror)
                else:
                    LOG.info("Removing file {0}".format(input_file))
                    try:
                        if self._dry_run:
                            LOG.debug("dry_run = True")
                        else:
                            os.remove(input_file)
                    except OSError:
                        message = "Cannot remove {0}".format(input_file)
                        LOG.error(message)
                        self.send_error_message(message, self.oid, self.uid)


class SystemMonitorApp(BarrierAppDROP, ErrorHandling):
    def __init__(self, oid, uid, **keywords):
        super(SystemMonitorApp, self).__init__(oid, uid, **keywords)
        self._sleep_time = 60
        self._cpu_count = None

    def initialize(self, **keywords):
        super(SystemMonitorApp, self).initialize(**keywords)
        self._session_id = self._getArg(keywords, "session_id", None)
        self._sleep_time = self._getArg(keywords, "sleep_time", 60)

    def dataURL(self):
        return type(self).__name__

    def run(self):
        self._cpu_count = psutil.cpu_count()

        while True:
            self._print_status()
            sleep(self._sleep_time)

    @staticmethod
    def _bytes2human(number):
        # http://code.activestate.com/recipes/578019
        # >>> bytes2human(10000)
        # '9.8K'
        # >>> bytes2human(100001221)
        # '95.4M'
        symbols = ("K", "M", "G", "T", "P", "E", "Z", "Y")
        prefix = {}
        for i, s in enumerate(symbols):
            prefix[s] = 1 << (i + 1) * 10
        for s in reversed(symbols):
            if number >= prefix[s]:
                value = float(number) / prefix[s]
                return "%.1f%s" % (value, s)
        return "%sB" % number

    def _print_status(self):
        text = "\n"
        text += self._get_cpu()
        text += self._get_memory()
        text += self._get_disk()
        text += self._get_network()

        LOG.info(text)

    def _get_cpu(self):
        text = "\nCPU\n"
        cpus_percent = psutil.cpu_percent(percpu=True)
        for i in range(self._cpu_count):
            text += "CPU %-6i" % i
        text += "\n"
        for percent in cpus_percent:
            text += "%-10s" % percent
        text += "\n"

        return text

    def _get_memory(self):
        def pprint_ntuple(nt):
            text1 = ""
            for name in nt._fields:
                value = getattr(nt, name)
                if name != "percent":
                    value = self._bytes2human(value)
                text1 += "%-10s : %7s\n" % (name.capitalize(), value)
            return text1

        text = "\nMEMORY\n------\n"
        text += pprint_ntuple(psutil.virtual_memory())
        text += "\nSWAP\n----\n"
        text += pprint_ntuple(psutil.swap_memory())
        text += "\n"
        return text

    def _get_disk(self):
        text = "\nDISK\n"
        templ = "%-17s %8s %8s %8s %5s%% %9s  %s\n"
        text += templ % ("Device", "Total", "Used", "Free", "Use ", "Type", "Mount")
        for part in psutil.disk_partitions(all=False):
            if os.name == "nt":
                if "cdrom" in part.opts or part.fstype == "":
                    # skip cd-rom drives with no disk in it; they may raise
                    # ENOENT, pop-up a Windows GUI error for a non-ready
                    # partition or just hang.
                    continue
            usage = psutil.disk_usage(part.mountpoint)
            text += templ % (
                part.device,
                self._bytes2human(usage.total),
                self._bytes2human(usage.used),
                self._bytes2human(usage.free),
                int(usage.percent),
                part.fstype,
                part.mountpoint,
            )

        return text

    def _get_network(self):
        text = "\nNETWORK\n"
        tot_after = psutil.net_io_counters()

        text += "total bytes:           sent: %-10s   received: %s\n" % (
            self._bytes2human(tot_after.bytes_sent),
            self._bytes2human(tot_after.bytes_recv),
        )

        text += "total packets:         sent: %-10s   received: %s\n" % (
            tot_after.packets_sent,
            tot_after.packets_recv,
        )

        return text
