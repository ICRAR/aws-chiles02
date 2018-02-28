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

import errno
import os


def pluralise(number, value):
    """
    Pluralise 'value' in the form "value is" or "values are"
    :param number: The number of things we're describing with 'value'
    :param value: The base word to pluralise
    :return: Pluralised value.
    """
    if number == 1 or number == -1:
        return "{0} is".format(value)
    else:
        return "{0}s are".format(value)


def make_path(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


class EventEmitter(object):
    """
    Add listeners with the 'on' method, remove them with 'off', and fire events with 'emit'
    """

    def __init__(self):
        """
        Initialise the EventEmitter
        """
        self._listeners = {}

    def on(self, event, listener):
        """
        Add a new event listener
        :param event: The event to listen on
        :param listener: The listener
        :return: self for chained api
        """
        current = self._listeners[event]

        if current is None:
            current = []
            self._listeners[event] = current

        if current.count(listener) == 1:
            return

        current.append(listener)

        return self

    def off(self, event, listener):
        """
        Remove an event listener
        :param event: The event to remove from
        :param listener: The listener function to remove
        :return: self for chained api
        """
        current = self._listeners[event]

        if current is None:
            return

        try:
            current.remove(listener)
        except:
            pass

        return self

    def emit(self, event, *args):
        """
        Emit an event
        :param event: The event to emit
        :param args: Arguments for the event
        :return: self for chained api
        """
        current = self._listeners[event]

        if current is None:
            return

        for callback in current:
            callback(*args)

        return self
