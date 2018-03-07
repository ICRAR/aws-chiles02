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
from configobj import ConfigObj
from save import BaseChilesGUIConfig
from utils import make_path


class ChilesGUIConfig(BaseChilesGUIConfig):
    autosave_history = 5

    def __init__(self, autosave_path="./autosave_aws-chiles-02.settings"):
        """
        Create a new config file handler.
        :param autosave_path: The path to the config file.
        """
        self.autosave_path = autosave_path

    def load(self):
        """
        Returns the current history of saved config items.
        Each item is returned as {name: [oldest, ..., newest]}
        :return: A dictionary containing histories of each config item.
        """
        # Load the config obj, and send it back as a dict.
        return {k: v for k, v in ConfigObj(self.autosave_path).iteritems()}

    def save(self, new_values):
        """
        Saves a set of new values to the config file.
        If a value in new_values matches one in the config file, it wont be saved.
        If the value is different, it will be added to the history array for that config file.
        The history array will be truncated to the last 'autosave_history' elements
        :param new_values: A dictionary of new values to save.
        """
        # First, load up the current config file state
        current = self.load()

        # Iterate over all the new values we have
        for k, v in new_values.iteritems():
            try:
                # Append new items to the list, if they're new in comparison to the last value
                current_list = current[k]
                if len(current_list) == 0 or current_list[len(current_list) - 1] != str(v):
                    current_list.append(v)

                # Remove old list items if the list is too long
                remove = len(current_list) - self.autosave_history
                if remove > 0:
                    current[k] = current_list[remove:]

            except KeyError:
                # Create list if one doesn't already exist
                current[k] = [v]

        # Build up the configobj from the current values
        config_obj = ConfigObj()
        for k, v in current.iteritems():
            config_obj[k] = v

        # Save the configobj to the autosave path
        make_path(os.path.dirname(self.autosave_path))
        with open(self.autosave_path, 'w') as f:
            config_obj.write(f)
