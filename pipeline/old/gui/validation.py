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

from utils import pluralise
from abc import *


class FieldValidationException(Exception):
    """
    Thrown whenever a single config option failed to validate.
    """
    def __init__(self, field, value, expected):
        super(Exception, self).__init__("Field {0} has invalid value type {1} expected {2}".format(field, value, expected))

        self.field = field
        self.value = value
        self.expected = expected


class ValidationException(Exception):
    """
    Thrown whenever a command is called, but the required config options for that command did not all validate.
    """
    def __init__(self, *args):
        super(Exception, self).__init__("Error validating data. {0} {1} invalid.".format(len(args), pluralise(len(args), "value")))
        self.exceptions = args


class Type:
    __metaclass__ = ABCMeta

    @abstractmethod
    def validate_and_convert(self, name, value):
        pass

    def validate_and_show_error(self, name, value, label):
        """
        Validates the given value, and sets the background of the given label
        to red if the value is invalid. The label is set back to white if the valid is valid.
        :param value: The value to validate
        :param label: The label to set if the value is invalid.
        :return:
        """
        try:
            value = self.validate_and_convert(name, value)
            label.config(background="white")
        except Exception:
            label.config(background="IndianRed1")


class Int(Type):

    def validate_and_convert(self, name, value):
        """
        Tries to convert the given value to the type required by this prototype.
        :param value: The value to convert
        :return: The converted value
        :exception: FieldValidationException if the value cannot be converted to the required type
        """
        try:
            return int(value)
        except Exception:
            raise FieldValidationException(name, value, "int")


class Float(Type):

    def validate_and_convert(self, name, value):
        """
        Tries to convert the given value to the type required by this prototype.
        :param value: The value to convert
        :return: The converted value
        :exception: FieldValidationException if the value cannot be converted to the required type
        """
        try:
            return float(value)
        except Exception:
            raise FieldValidationException(name, value, "float")


class String(Type):

    def validate_and_convert(self, name, value):
        """
        Tries to convert the given value to the type required by this prototype.
        :param value: The value to convert
        :return: The converted value
        :exception: FieldValidationException if the value cannot be converted to the required type
        """
        try:
            return str(value)
        except Exception:
            raise FieldValidationException(name, value, "str")


class Bool(Type):

    def validate_and_convert(self, name, value):
        """
        Tries to convert the given value to the type required by this prototype.
        :param value: The value to convert
        :return: The converted value
        :exception: FieldValidationException if the value cannot be converted to the required type
        """
        try:
            if type(value) == str:
                if value == "True":
                    return True
                elif value == "False":
                    return False
                else:
                    raise FieldValidationException(name, value, "bool")
            else:
                return bool(value)
        except Exception:
            raise FieldValidationException(name, value, "bool")


class SelectList(String):
    def __init__(self, options):
        super(SelectList, self).__init__()
        self.options = options

    def validate_and_convert(self, name, value):
        if value not in self.options:
            raise FieldValidationException(name, value, "one of {0}".format(self.options))

        return super(SelectList, self).validate_and_convert(name, value)
