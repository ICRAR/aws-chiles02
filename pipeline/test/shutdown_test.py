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
Test shutting down a server from DFMS
"""
import json
import logging

import argparse

from aws_chiles02.apps import BashShellApp
from aws_chiles02.common import get_session_id, get_oid, get_uid, get_module_name
from dfms.drop import dropdict
from dfms.manager.client import SetEncoder, NodeManagerClient

LOG = logging.getLogger(__name__)


class BuildGraph:
    def __init__(self):
        self._drop_list = []
        self._start_oids = []

    def build_graph(self):
        start_drop = dropdict({
            "type": 'plain',
            "storage": 'memory',
            "oid": get_oid('memory_in'),
            "uid": get_uid(),
        })
        self._start_oids.append(start_drop['uid'])
        self.append(start_drop)

        shutdown_drop = dropdict({
            "type": 'app',
            "app": get_module_name(BashShellApp),
            "oid": get_oid('app_bash_shell_app'),
            "uid": get_uid(),
            "command": 'sudo shutdown -h +5 "DFMS node shutting down"',
            "user": 'root',
            "input_error_threshold": 100,
        })
        shutdown_drop.addInput(start_drop)
        self.append(shutdown_drop)

    @property
    def drop_list(self):
        return self._drop_list

    @property
    def start_oids(self):
        return self._start_oids

    def append(self, drop):
        self._drop_list.append(drop)


def command_json(args):
    LOG.info(args)
    graph = BuildGraph()
    graph.build_graph()
    json_dumps = json.dumps(graph.drop_list, indent=2, cls=SetEncoder)
    LOG.info(json_dumps)
    with open("/tmp/json_split.txt", "w") as json_file:
        json_file.write(json_dumps)


def command_run(args):
    LOG.info(args)
    graph = BuildGraph()
    graph.build_graph()

    client = NodeManagerClient(args.host, args.port)

    session_id = get_session_id()
    client.create_session(session_id)
    client.append_graph(session_id, graph.drop_list)
    client.deploy_session(session_id, graph.start_oids)


def parser_arguments():
    parser = argparse.ArgumentParser('Build the shutdown physical graph')

    subparsers = parser.add_subparsers()

    parser_json = subparsers.add_parser('json', help='display the json')
    parser_json.set_defaults(func=command_json)

    parser_run = subparsers.add_parser('run', help='run and deploy')
    parser_run.add_argument('host', help='the host the dfms is running on')
    parser_run.add_argument('-p', '--port', type=int, help='the port to bind to', default=8000)
    parser_run.set_defaults(func=command_run)

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    arguments = parser_arguments()
    arguments.func(arguments)
