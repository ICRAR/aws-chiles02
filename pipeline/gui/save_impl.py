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

import os
import re
from configobj import ConfigObj
from save import BaseChilesGUIConfig
from utils import make_path


class ChilesGUIConfig(BaseChilesGUIConfig):
    autosave_format = "{0}_autosave_aws-chiles-02.settings"
    autosave_regex = re.compile('([0-9]+)(?=_autosave)')  # Get the number from the front of the autosave name, but don't include anything after
    autosave_history = 5

    def __init__(self, autosave_path):
        self.autosave_path = autosave_path

    def save(self, filename, config):
        """
        Saves the provided config to a file
        :param filename:
        :param config:
        :return:
        """
        config_obj = ConfigObj()
        # Copy everything into the config obj
        for k, v in config.iteritems():
            config_obj[k] = v

        make_path(os.path.dirname(filename))

        with open(filename, 'w') as f:
            config_obj.write(f)

    def load(self, filename):
        """
        Loads the config from the specified file
        :param filename:
        :return:
        """
        config_obj = ConfigObj(filename)

        return {k: v for k, v in config_obj.iteritems()}  # Don't send back the ConfigObj, send back a dict instead

    def autoload(self):
        """
        Loads the latest saved config
        :return:
        """
        saves = self.autosave_list()

        if len(saves) > 0:
            return self.load(os.path.join(self.autosave_path, saves[0][0]))
        else:
            return None

    def autosave(self, config):
        """
        Autosave the provided config, making it the la
        :param config:
        :return:
        """
        saves = self.autosave_list()

        if len(saves) > 0:
            # Increment latest save name
            filename = self.autosave_format.format(saves[0][1] + 1)

            # We've hit the max autosave filename, so delete the oldest saves
            while len(saves) >= self.autosave_history:
                os.remove(os.path.join(self.autosave_path, saves.pop()[0]))
        else:
            # No saves
            filename = self.autosave_format.format(1)

        self.save(os.path.join(self.autosave_path, filename), config)

    def autosave_list(self, **kwargs):
        """
        List all saved configs
        :return:
        """
        files = []
        absolute = False

        if 'absolute' in kwargs:
            absolute = kwargs['absolute']

        if not os.path.exists(self.autosave_path):
            return files

        for filename in os.listdir(self.autosave_path):
            number = self.autosave_regex.match(filename)

            if number is not None:
                if absolute:
                    filename = os.path.join(self.autosave_path, filename)
                files.append((filename, int(number.group(0))))

        # Sort so that the latest is first
        if len(files) > 0:
            files.sort(key=lambda x: x[1], reverse=True)

        return files
