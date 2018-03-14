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
from abc import *
from validation import Bool, String, SelectList
from option_instance import InputInstance, SelectInstance, CheckInstance, ChooseFileInstance


class OptionPrototype:
    """
    Represents a prototype of a single configuration option. These are used to
    build tkinter objects.
    """
    __metaclass__ = ABCMeta

    def __init__(self, id, name, datatype, **kwargs):
        """
        Create a new ConfigOption
        :param id: The unique identifier of the option.
        :param name: The shown label name for the option.
        :param datatype: The datatype for this option (Int, String, Bool)
        :param kwargs: Keyword arguments for the option. default or data_type are valid.
        """
        self.id = id
        self.name = name
        self.datatype = datatype
        self.default = ''

        if "default" in kwargs:
            self.default = self.datatype.validate_and_convert(self.name, kwargs['default'])

    @abstractmethod
    def create(self, parent):
        pass

    def get_default(self):
        """
        :exception FieldValidationException: If the default value can't be converted to this prototype's type.
        :return: The default value for this prototype
        """
        return self.validate_and_convert(self.default)

    def validate_and_convert(self, value):
        """
        Converts a value to this prototype's field type, and returns it.
        If the field fails to validate, then a FieldValidationException is thrown
        :param value: The value to validate and convert
        :exception FieldValidationException: If the provided value cannot be converted to this prototype's type.
        :return: Validated value, converted to the prototype's type
        """
        return self.datatype.validate_and_convert(self.name, value)

    def validate_and_show_error(self, value, tk_field):
        """
        Converts a value to this prototype's field type, and returns it.
        If the value could not convert, the 'tk_field' will be highlighted in an error colour
        to signify to the user that the field failed to write.
        :param value:
        :param tk_field:
        :return:
        """
        return self.datatype.validate_and_show_error(self.name, value, tk_field)


class InputPrototype(OptionPrototype):
    """
    Represents an input field that the user can type values into.
    Whenever the input field is updated, its value is validated.
    If the user's provided valid is invalid, the field is highlighted in red to inform them.
    """
    option_type = "Input"

    def create(self, parent):
        return InputInstance(self, parent)


class SelectPrototype(OptionPrototype):
    """
    Represents a drop down list with a set of selection options.
    The user can select any option from the list.
    """
    option_type = "Select"

    def __init__(self, id, name, options, **kwargs):
        super(SelectPrototype, self).__init__(id, name, SelectList(options), **kwargs)
        self.options = options

    def create(self, parent):
        return SelectInstance(self, parent)


class CheckPrototype(OptionPrototype):
    """
    Represents a checkbox that the user can either set to checked or unchecked.
    """
    option_type = "Check"

    def __init__(self, id, name, **kwargs):
        if 'default' not in kwargs:
            kwargs['default'] = False
        super(CheckPrototype, self).__init__(id, name, Bool(), **kwargs)

    def create(self, parent):
        return CheckInstance(self, parent)


class ChooseFilePrototype(OptionPrototype):
    """
    Represents an input field with a Browse... button that the user can use to select
    a local file on their machine.
    """
    option_type = "ChooseFile"

    def __init__(self, id, name, **kwargs):
        super(ChooseFilePrototype, self).__init__(id, name, String(), **kwargs)

    def create(self, parent):
        return ChooseFileInstance(self, parent)
