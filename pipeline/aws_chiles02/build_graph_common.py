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
The abstract graph builder
"""
import os
import uuid
from abc import ABCMeta, abstractmethod

from aws_chiles02.apps_general import CopyLogFilesApp
from aws_chiles02.common import get_module_name
from dfms.apps.bash_shell_app import BashShellApp
from dfms.drop import dropdict, DirectoryContainer, BarrierAppDROP


class AbstractBuildGraph:
    # This ensures that:
    #  - This class cannot be instantiated
    #  - Subclasses implement methods decorated with @abstractmethod
    __metaclass__ = ABCMeta

    def __init__(self, bucket_name, shutdown, node_details, volume, session_id, dim_ip):
        self._drop_list = []
        self._start_oids = []
        self._map_carry_over_data = {}
        self._bucket_name = bucket_name
        self._shutdown = shutdown
        self._bucket = None
        self._node_details = node_details
        self._volume = volume
        self._session_id = session_id
        self._dim_ip = dim_ip
        self._counters = {}

        for key, list_ips in self._node_details.iteritems():
            for instance_details in list_ips:
                self._map_carry_over_data[instance_details['ip_address']] = self.new_carry_over_data()

    @property
    def drop_list(self):
        return self._drop_list

    @property
    def start_oids(self):
        return self._start_oids

    def add_drop(self, drop):
        self._drop_list.append(drop)

    def get_oid(self, count_type):
        count = self._counters.get(count_type)
        if count is None:
            count = 1
        else:
            count += 1
        self._counters[count_type] = count

        return '{0}__{1:06d}'.format(count_type, count)

    @staticmethod
    def get_uuid():
        return str(uuid.uuid4())

    def copy_logfiles_and_shutdown(self, shutdown_dim=False):
        """
        Copy the logfile to S3 and shutdown
        """
        if shutdown_dim:
            dim_shutdown_drop = self.create_bash_shell_app(self._dim_ip, 'sudo shutdown -h +5 "DFMS node shutting down" &')

        for list_ips in self._node_details.values():
            for instance_details in list_ips:
                node_id = instance_details['ip_address']

                copy_log_drop = self.create_app(node_id, get_module_name(CopyLogFilesApp), 'copy_log_files_app')

                # After everything is complete
                for drop in self._drop_list:
                    if drop['type'] in ['plain', 'container'] and drop['node'] == node_id:
                        copy_log_drop.addInput(drop)

                s3_drop_out = self.create_s3_drop(
                    node_id,
                    self._bucket_name,
                    '{0}/{1}.tar'.format(
                        self._session_id,
                        node_id,
                    ),
                    'aws-chiles02',
                    oid='s3_out'
                )
                copy_log_drop.addOutput(s3_drop_out)

                if self._shutdown:
                    shutdown_drop = self.create_bash_shell_app(node_id, 'sudo shutdown -h +5 "DFMS node shutting down" &')
                    shutdown_drop.addInput(s3_drop_out)

                    if shutdown_dim:
                        memory_drop = self.create_memory_drop(node_id)
                        shutdown_drop.addOutput(memory_drop)

                        dim_shutdown_drop.addInput(memory_drop)

    def tag_all_app_drops(self, tags):
        for drop in self._drop_list:
            if drop['type'] == 'app':
                drop.update(tags)

    @abstractmethod
    def build_graph(self):
        """
        Build the graph
        """

    @abstractmethod
    def new_carry_over_data(self):
        """"
        Get the carry over data structure
        """

    def create_bash_shell_app(self, node_id, command, oid='bash_shell_app', input_error_threshold=100):
        oid_text = self.get_oid(oid)
        uid_text = self.get_uuid()
        drop = dropdict({
            "type": 'app',
            "app": get_module_name(BashShellApp),
            "oid": oid_text,
            "uid": uid_text,
            "command": command,
            "input_error_threshold": input_error_threshold,
            "node": node_id,
        })
        self.add_drop(drop)
        return drop

    def create_barrier_app(self, node_id, oid='barrier_app', input_error_threshold=100):
        oid_text = self.get_oid(oid)
        uid_text = self.get_uuid()
        drop = dropdict({
            "type": 'app',
            "app": get_module_name(BarrierAppDROP),
            "oid": oid_text,
            "uid": uid_text,
            "input_error_threshold": input_error_threshold,
            "node": node_id,
        })
        self.add_drop(drop)
        return drop

    def create_app(self, node_id, app, oid, input_error_threshold=100, **key_word_arguments):
        oid_text = self.get_oid(oid)
        uid_text = self.get_uuid()
        drop = dropdict({
            "type": 'app',
            "app": app,
            "oid": oid_text,
            "uid": uid_text,
            "input_error_threshold": input_error_threshold,
            "node": node_id,
        })
        drop.update(key_word_arguments)
        self.add_drop(drop)
        return drop

    def create_docker_app(self, node_id, app, oid, image, command, user='ec2-user', input_error_threshold=100, **key_word_arguments):
        oid_text = self.get_oid(oid)
        uid_text = self.get_uuid()
        drop = dropdict({
            "type": 'app',
            "app": app,
            "oid": oid_text,
            "uid": uid_text,
            "image": image,
            "command": command,
            "user": user,
            "input_error_threshold": input_error_threshold,
            "node": node_id,
        })
        drop.update(key_word_arguments)
        self.add_drop(drop)
        return drop

    def create_directory_container(self, node_id, oid='directory_container', expire_after_use=True):
        oid_text = self.get_oid(oid)
        uid_text = self.get_uuid()
        drop = dropdict({
            "type": 'container',
            "container": get_module_name(DirectoryContainer),
            "oid": oid_text,
            "uid": uid_text,
            "precious": False,
            "dirname": os.path.join(self._volume, oid_text),
            "check_exists": False,
            "expireAfterUse": expire_after_use,
            "node": node_id,
        })
        self.add_drop(drop)
        return drop

    def create_memory_drop(self, node_id, oid='memory_drop'):
        oid_text = self.get_oid(oid)
        uid_text = self.get_uuid()
        drop = dropdict({
            "type": 'plain',
            "storage": 'memory',
            "oid": oid_text,
            "uid": uid_text,
            "precious": False,
            "node": node_id,
        })
        self.add_drop(drop)
        return drop

    def create_s3_drop(self, node_id, bucket_name, key, profile_name, oid='s3'):
        oid_text = self.get_oid(oid)
        uid_text = self.get_uuid()
        drop = dropdict({
            "type": 'plain',
            "storage": 's3',
            "oid": oid_text,
            "uid": uid_text,
            "expireAfterUse": True,
            "precious": False,
            "bucket": bucket_name,
            "key": key,
            "profile_name": profile_name,
            "node": node_id,
        })
        self.add_drop(drop)
        return drop

    def create_json_drop(self, node_id, oid='json'):
        oid_text = self.get_oid(oid)
        uid_text = self.get_uuid()
        drop = dropdict({
            "type": 'plain',
            "storage": 'json',
            "oid": oid_text,
            "uid": uid_text,
            "precious": False,
            "dirname": os.path.join(self._volume, oid_text),
            "check_exists": False,
            "node": node_id,
        })
        self.add_drop(drop)
        return drop

    def create_file_drop(self, node_id, filepath, oid='file'):
        oid_text = self.get_oid(oid)
        uid_text = self.get_uuid()
        drop = dropdict({
            "type": 'plain',
            "storage": 'file',
            "oid": oid_text,
            "uid": uid_text,
            "precious": False,
            "filepath": filepath,
            "node": node_id,
        })
        self.add_drop(drop)
        return drop
