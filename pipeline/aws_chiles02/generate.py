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
Provide a GIT like interface for the generation code
"""
import argparse
import sys

from aws_chiles02 import generate_clean_graph, generate_concatenate_graph, generate_jpeg2000_graph, generate_mstransform_graph, generate_uvsub_graph


class Generate(object):
    def __init__(self):
        self._arguments = None
        parser = argparse.ArgumentParser(
            description='Start the various generate scripts',
            usage='''generate <command> [<args>]

    The most commonly used generate commands are:
       clean       Record changes to the repository
       concatenate Download objects and refs from another repository
       jpeg2000    Produce the files required for JPEG2000
       mstransform Split the input data into small chunks
       uvsub       Perform the sky model subtraction

    The normal order is:
       mstransform
       uvsub
       clean
       jpeg2000
       concatenate
    ''')
        parser.add_argument('command', help='Subcommand to run')

        # parse_args defaults to [1:] for args, but I need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print 'Unrecognized command'
            parser.print_help()
            exit(1)

        # use dispatch pattern to invoke method with same name
        getattr(self, args.command)()

    def _run_arguments(self):
        self._arguments.func(self._arguments)

    def clean(self):
        self._arguments = generate_clean_graph.parser_arguments(sys.argv[2:])
        self._run_arguments()

    def concatenate(self):
        self._arguments = generate_concatenate_graph.parser_arguments(sys.argv[2:])
        self._run_arguments()

    def jpeg2000(self):
        self._arguments = generate_jpeg2000_graph.parser_arguments(sys.argv[2:])
        self._run_arguments()

    def mstransform(self):
        self._arguments = generate_mstransform_graph.parser_arguments(sys.argv[2:])
        self._run_arguments()

    def uvsub(self):
        self._arguments = generate_uvsub_graph.parser_arguments(sys.argv[2:])
        self._run_arguments()


if __name__ == '__main__':
    Generate()
