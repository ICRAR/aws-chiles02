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


class GetArguments:
    def __init__(self, config):
        self._config = config

    @staticmethod
    def readline_input(prompt, text=''):
        def hook():
            readline.insert_text(text)
            readline.redisplay()
        readline.set_pre_input_hook(hook)
        result = input(prompt)
        readline.set_pre_input_hook()
        return result

    def get(self, key, prompt, help_text=None, data_type=None, default=None, allowed=None, use_stored=True):
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
                    print('\n' + help_text + '\n')
                else:
                    print('\nNo help available\n')

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
