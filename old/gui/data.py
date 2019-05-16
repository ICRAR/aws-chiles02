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
    """
    Thrown whenever the add_read or add_write functions attempt to add using a duplicate ID.
    """
    pass


class DataAccess:

    def __init__(self):
        """
        Create a new DataAccess instance
        """
        self.read_functions = {}
        self.write_functions = {}

    def add_read(self, name, func):
        """
        Add a read function to this data access.
        :param name: Unique ID for this read function.
        :param func: Function to call to read data.
        :exception DuplicateDataField: If a read function already exists for this field.
        """
        if name in self.read_functions:
            raise DuplicateDataField("Read: {0}".format(name))

        self.read_functions[name] = func

    def add_write(self, name, func):
        """
        Add a write function to this data access.
        :param name: Unique ID for this write function.
        :param func: Function to call to write data. Must accept a single parameter
        :exception DuplicateDataField: If a write function already exists for this field.
        """
        if name in self.write_functions:
            raise DuplicateDataField("Write: {0}".format(name))

        self.write_functions[name] = func

    def clear(self, name=None):
        """
        Clear all read and write functions, or functions for a specific ID.
        :param name: The Unique ID to remove functions for, or None to remove all functions for all IDs.
        """
        if name is None:
            # Remove all read and write functions
            self.read_functions.clear()
            self.write_functions.clear()
        else:
            # Try and remove only read and write functions associated with the specified id
            try:
                del self.read_functions[name]
                del self.write_functions[name]
            except KeyError:
                pass

    def read(self, *args):
        """
        Read the specified values using the registered read functions.
        :param args: A list of IDs representing the read functions to call. Pass nothing to read all.
        :exception ValidationException: If errors occur while validating the data to read.
        :return: A dictionary containing the values in the form {id: value}
        """
        out = {}
        errors = []

        if len(args) == 0:
            # If provided with no args, read everything
            args = self.read_functions.iterkeys()

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
        Write the specified values using the registered write functions.
        :param kwargs: A dictionary of ID to value pairs to write.
        :exception ValidationException: If errors occur while validating the data to write.
        """
        errors = []

        for item, value in kwargs.iteritems():
            try:
                self.write_functions[item](value)
            except FieldValidationException as e:
                errors.append(e)

        if len(errors):
            raise ValidationException(*errors)
