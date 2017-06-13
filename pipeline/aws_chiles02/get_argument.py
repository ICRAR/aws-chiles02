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
Get command line arguments
"""
import readline

from aws_chiles02.common import TTY


class GetArguments:
    def __init__(self, config, mode):
        self._config = config
        self._mode = mode

    @staticmethod
    def readline_input(prompt, prefill=''):
        readline.set_startup_hook(lambda: readline.insert_text(prefill))
        try:
            return raw_input(prompt)
        finally:
            readline.set_startup_hook()

    def get(self, key, prompt, help_text=None, data_type=None, default=None, allowed=None, use_stored=True):
        if self._mode == TTY:
            self._get_tty(key, prompt, help_text=help_text, data_type=data_type, default=default, allowed=allowed, use_stored=use_stored)
        else:
            self._get_not_tty(key, prompt, help_text=help_text, data_type=data_type, default=default, allowed=allowed, use_stored=use_stored)

    def _get_not_tty(self, key, prompt, help_text=None, data_type=None, default=None, allowed=None, use_stored=True):
        if key in self._config and use_stored:
            from_config = self._config[key]
        else:
            from_config = None

        if from_config is not None and default is not None:
            prompt = '{0} [{1}](default: {2}):'.format(prompt, from_config, default)
        elif from_config is not None:
            prompt = '{0} [{1}]:'.format(prompt, from_config)
        elif default is not None:
            prompt = '{0} [{1}]:'.format(prompt, default)
        else:
            prompt = '{0}:'.format(prompt)

        data = None

        while data is None:
            try:
                data = raw_input(prompt)
            except EOFError:
                if from_config is not None:
                    data = from_config
                else:
                    data = default
            if data == '?':
                if help_text is not None:
                    print '\n' + help_text + '\n'
                else:
                    print '\nNo help available\n'

                data = None
            elif data == '':
                if from_config is not None:
                    data = from_config
                else:
                    data = default

            if allowed is not None:
                if data not in allowed:
                    data = None

        if data_type is not None:
            if data_type == int:
                self._config[key] = int(data)
            elif data_type == float:
                self._config[key] = float(data)
            elif data_type == bool:
                self._config[key] = data in ['True', 'true', 'Yes', 'yes']
        else:
            self._config[key] = data

    def _get_tty(self, key, prompt, help_text=None, data_type=None, default=None, allowed=None, use_stored=True):
        if key in self._config and use_stored:
            from_config = self._config[key]
        else:
            from_config = None

        if from_config is None:
            prefill = ''
        else:
            prefill = from_config

        if default is not None:
            prompt = '{0} (default: {1}):'.format(prompt, default)
        else:
            prompt = '{0}:'.format(prompt)

        data = None

        while data is None:
            data = self.readline_input(prompt, prefill)
            if data == '?':
                if help_text is not None:
                    print '\n' + help_text + '\n'
                else:
                    print '\nNo help available\n'

                data = None

            if allowed is not None:
                if data not in allowed:
                    data = None

        if data_type is not None:
            if data_type == int:
                self._config[key] = int(data)
            elif data_type == float:
                self._config[key] = float(data)
            elif data_type == bool:
                self._config[key] = data in ['True', 'true', 'Yes', 'yes']
        else:
            self._config[key] = data
