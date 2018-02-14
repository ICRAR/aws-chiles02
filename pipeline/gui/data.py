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

from validation import FieldValidationException, ValidationException


class DuplicateDataField(Exception):
    pass


class DataAccess:

    def __init__(self):
        """

        """
        self.read_functions = {}
        self.write_functions = {}

    def add_read(self, name, func):
        """

        :param name:
        :param function:
        :return:
        """
        if name in self.read_functions:
            raise DuplicateDataField("Read: {0}".format(name))

        self.read_functions[name] = func

    def add_write(self, name, func):
        """

        :param name:
        :param function:
        :return:
        """
        if name in self.write_functions:
            raise DuplicateDataField("Write: {0}".format(name))

        self.write_functions[name] = func

    def read(self, *args):
        """

        :param args:
        :return:
        """
        out = {}
        errors = []

        # Try to read all of the values requested by the user using our read functions
        for item in args:
            try:
                out[item] = self.read_functions[item]()
            except FieldValidationException as e:
                errors.append(e)

        if len(errors):
            # If any reads fail, throw the validation exception to report back to the user as to why they failed.
            raise ValidationException(*errors)

        return out

    def write(self, **kwargs):
        """

        :param kwargs:
        :return:
        """
        errors = []

        for item, value in kwargs.iteritems():
            try:
                self.write_functions[item](value)
            except FieldValidationException as e:
                errors.append(e)

        if len(errors):
            raise ValidationException(*errors)