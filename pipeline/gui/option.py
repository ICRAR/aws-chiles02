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
import Tkinter as tk
import tkFileDialog
from abc import *
from validation import Bool, String, SelectList
from functools import partial


class Option:
    """
    Represents a prototype of a single configuration option. These are used to
    build tkinter objects.
    """

    __metaclass__ = ABCMeta

    def __init__(self, id, name, type, **kwargs):
        """
        Create a new ConfigOption
        :param id: The unique identifier of the option.
        :param name: The shown label name for the option.
        :param t: The type of the option (input, select, check, file)
        :param kwargs: Keyword arguments for the option. default or data_type are valid.
        """
        self.id = id
        self.name = name
        self.type = type
        self.default = ''

        if "default" in kwargs:
            self.default = self.type.validate_and_convert(self.name, kwargs['default'])

    @staticmethod
    def file_picker(text):
        """
        Opens a file picker dialogue, and sets the selected value in the provided StringVar
        :param text: A tkinter string var to set the selected path for.
        """
        text.set(tkFileDialog.asksaveasfilename(title="Choose a file"))

    @abstractmethod
    def create(self, parent, row, access, history):
        pass

    def create_history_menu(self, parent, history, write):
        var = tk.StringVar(value="History...")
        empty = False
        if len(history):
            history[0] = history[0] + " (latest)"
        else:
            history = ["None"]
            empty = True

        history_menu = tk.OptionMenu(parent, var, *history)

        def write_value(name, index, mode):
            if not empty:
                value = var.get()
                if value == history[0]:
                    value = value[:-len(" (latest)")]
                write(value)

            var.set("History...")
            history_menu.update()

        var.trace("w", write_value)

        return history_menu


class Input(Option):

    def create(self, parent, row, access, history):
        """
        Creates a tkinter object from this prototype.
        :param parent: The parent for the object
        :param row: The current row in the parent this object should be placed in
        :param data_read: The dictionary that contains data access functions.
        :param data_write: The dictionary that contains data writing functions.
        """
        label = tk.Label(parent, text=self.name + ":", justify=tk.LEFT)
        label.grid(row=row, sticky=tk.W)

        var = tk.StringVar(parent, self.type.validate_and_convert(self.name, self.default))
        e = tk.Entry(parent, textvariable=var)

        def write(value):
            var.set(self.type.validate_and_convert(self.name, value))

        def read():
            return self.type.validate_and_convert(self.name, var.get())

        def trace_write(name, index, mode):
            return self.type.validate_and_show_error(self.name, var.get(), e)

        access.add_read(self.id, read)
        access.add_write(self.id, write)
        var.trace('w', trace_write)

        e.grid(row=row, column=1)

        menu = self.create_history_menu(parent, history, write)
        menu.grid(row=row, column=2)


class Select(Option):

    def __init__(self, id, name, options, **kwargs):
        super(Select, self).__init__(id, name, SelectList(options), **kwargs)
        self.options = options

    def create(self, parent, row, access, history):
        """
        Creates a tkinter object from this prototype.
        :param parent: The parent for the object
        :param row: The current row in the parent this object should be placed in
        """
        label = tk.Label(parent, text=self.name + ":", justify=tk.LEFT)
        label.grid(row=row, sticky=tk.W)

        var = tk.StringVar(parent, self.type.validate_and_convert(self.name, self.default))
        e = tk.OptionMenu(parent, var, *self.options)

        def write(value):
            var.set(self.type.validate_and_convert(self.name, value))

        def read():
            return self.type.validate_and_convert(self.name, var.get())

        access.add_read(self.id, read)
        access.add_write(self.id, write)

        e.grid(row=row, column=1)

        menu = self.create_history_menu(parent, history, write)
        menu.grid(row=row, column=2)


class Check(Option):

    def __init__(self, id, name, **kwargs):
        if 'default' not in kwargs:
            kwargs['default'] = False
        super(Check, self).__init__(id, name, Bool(), **kwargs)

    def create(self, parent, row, access, history):
        """
        Creates a tkinter object from this prototype.
        :param parent: The parent for the object
        :param row: The current row in the parent this object should be placed in
        """
        label = tk.Label(parent, text=self.name + ":", justify=tk.LEFT)
        label.grid(row=row, sticky=tk.W)

        var = tk.IntVar(parent, bool(self.default))
        e = tk.Checkbutton(parent, variable=var)

        def read():
            return var.get() == 1

        def write(value):
            var.set(self.type.validate_and_convert(self.name, value))

        access.add_read(self.id, read)
        access.add_write(self.id, write)

        e.grid(row=row, column=1)

        menu = self.create_history_menu(parent, history, write)
        menu.grid(row=row, column=2)


class ChooseFile(Option):

    def __init__(self, id, name, **kwargs):
        super(ChooseFile, self).__init__(id, name, String(), **kwargs)

    def create(self, parent, row, access, history):
        """
        Creates a tkinter object from this prototype.
        :param parent: The parent for the object
        :param row: The current row in the parent this object should be placed in
        """
        label = tk.Label(parent, text=self.name + ":", justify=tk.LEFT)
        label.grid(row=row, sticky=tk.W)

        e = tk.Frame(parent)
        var = tk.StringVar(parent, str(self.default))
        child = tk.Entry(e, textvariable=var)
        child.pack(side=tk.LEFT, padx=2, pady=2, fill=tk.X, expand=True)
        child = tk.Button(e, text="Browse...", command=partial(self.file_picker, var))
        child.pack(side=tk.LEFT, padx=2, pady=2, fill=tk.X, expand=True)

        def write(value):
            var.set(self.type.validate_and_convert(self.name, value))

        def read():
            return self.type.validate_and_convert(self.name, var.get())

        access.add_read(self.id, read)
        access.add_write(self.id, write)

        e.grid(row=row, column=1)

        menu = self.create_history_menu(parent, history, write)
        menu.grid(row=row, column=2)
