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
import os
from abc import *


def menu_options_count(menu):
    return menu.index("end")


def menu_get_options(menu):
    last = menu.index("end")
    return [menu.entrycget(index, "label") for index in range(last + 1)]


def menu_clear_options(menu):
    last = menu.index("end")
    if last is not None:
        menu.delete(0, last)


def menu_set_options(menu, var, options):
    if menu_options_count(menu) is not None:
        menu_clear_options(menu)

    for item in options:
        menu.add_command(label=item, command=tk._setit(var, item))


class OptionInstance(object):
    """
    Represents an instance of a single option. These contain the tkinter objects
    that are displayed in the GUI.
    """
    __metaclass__ = ABCMeta

    def __init__(self, parent):
        self.prototype = None
        self.parent = parent

        self.label_string = tk.StringVar(parent)
        self.label = tk.Label(parent, textvariable=self.label_string, justify=tk.LEFT)

        self.history_menu_string = tk.StringVar(value="History...")
        self.history_menu_string_trace = None
        self.history_menu = tk.OptionMenu(parent, self.history_menu_string, "None")
        self.history_menu_skip_set = False  # Used to allow the History dropdown box to revert to saying "History..." after an option is selected

    @abstractmethod
    def write(self, value):
        pass

    @abstractmethod
    def read(self):
        pass

    def position(self, row, column):
        self.label.grid(row=row, column=column, sticky=tk.W)
        self.history_menu.grid(row=row, column=column + 2, sticky=tk.EW)

    def unposition(self):
        self.label.grid_forget()
        self.history_menu.grid_forget()

    def set_history(self, history):
        """
        Set the history for this option instance's history dropdown.
        :param history: Array of history elements.
        """
        # First, determine if we have history to show
        empty = False
        if len(history):
            # We have history, and the latest value is the first in the list.
            self.write(history[0]) # Use the latest value from our history as our value
            history[0] = history[0] + " (latest)"
        else:
            # No history, so just show none
            history = ["None"]
            empty = True

        # Create the callback for whenever the history menu is modified.
        # This will do nothing if the menu is empty
        self.history_menu_skip_set = False

        def write_value(name, index, mode):
            if self.history_menu_skip_set:  # Ignore if we've been told to skip this value write
                self.history_menu_skip_set = False
                return

            if not empty:  # Write the value into the field if the history dropdown is not empty
                value = self.history_menu_string.get()
                if value == history[0]:
                    value = value[:-len(" (latest)")]
                self.write(value)
            self.history_menu_skip_set = True
            self.history_menu.after(0, lambda: self.history_menu_string.set("History..."))

        # Remove the old history string trace if one is present
        if self.history_menu_string_trace is not None:
            self.history_menu_string.trace_vdelete("w", self.history_menu_string_trace)
        # Add a new history string trace with the new write_value function
        self.history_menu_string_trace = self.history_menu_string.trace("w", write_value)

        menu_set_options(self.history_menu["menu"], self.history_menu_string, history)

    def set_prototype(self, prototype):
        self.prototype = prototype
        self.label_string.set(self.prototype.name + ":")


class InputInstance(OptionInstance):

    def __init__(self, prototype, parent):
        """
        Create a new InputInstance
        :param parent: The parent frame for this instance.
        """
        super(InputInstance, self).__init__(parent)

        self.entry_string = tk.StringVar(parent)
        self.entry = tk.Entry(parent, textvariable=self.entry_string, width=25)

        # If the entry string is given a bad value, highlight the field in red.
        self.entry_string.trace('w', lambda name, index, mode: self.prototype.validate_and_show_error(self.entry_string.get(), self.entry))

        self.set_prototype(prototype)

    def write(self, value):
        """
        Set the value of this input instance.
        :param value: The value to set to.
        """
        self.entry_string.set(self.prototype.validate_and_convert(value))

    def read(self):
        """
        Get the value of this input instance.
        :return:
        """
        return self.prototype.validate_and_convert(self.entry_string.get())

    def position(self, row, column):
        """
        Position this input instance at the specified row.
        :param row: The row to use
        :return:
        """
        super(InputInstance, self).position(row, column)
        self.entry.grid(row=row, column=column + 1)

    def unposition(self):
        """
        Unposition this input instance.
        :return:
        """
        super(InputInstance, self).unposition()
        self.entry.grid_forget()

    def set_prototype(self, prototype):
        """
        Set the prototype that this input instance should use.
        :param prototype:
        :return:
        """
        super(InputInstance, self).set_prototype(prototype)
        self.entry_string.set(self.prototype.get_default())


class SelectInstance(OptionInstance):

    def __init__(self, prototype, parent):
        super(SelectInstance, self).__init__(parent)
        self.menu_string = tk.StringVar(parent)
        self.menu = tk.OptionMenu(parent, self.menu_string, None)

        self.set_prototype(prototype)

    def write(self, value):
        self.menu_string.set(self.prototype.validate_and_convert(value))

    def read(self):
        return self.prototype.validate_and_convert(self.menu_string.get())

    def position(self, row, column):
        super(SelectInstance, self).position(row, column)
        self.menu.grid(row=row, column=column + 1)

    def unposition(self):
        super(SelectInstance, self).unposition()
        self.menu.grid_forget()

    def set_prototype(self, prototype):
        super(SelectInstance, self).set_prototype(prototype)
        self.menu_string.set(self.prototype.get_default())
        menu_set_options(self.menu["menu"], self.menu_string, self.prototype.options)


class CheckInstance(OptionInstance):

    def __init__(self, prototype, parent):
        super(CheckInstance, self).__init__(parent)

        self.check_int = tk.IntVar(parent)
        self.check = tk.Checkbutton(parent, variable=self.check_int)

        self.set_prototype(prototype)

    def write(self, value):
        self.check_int.set(self.prototype.validate_and_convert(value))

    def read(self):
        return self.check_int.get() == 1

    def position(self, row, column):
        super(CheckInstance, self).position(row, column)
        self.check.grid(row=row, column=column + 1)

    def unposition(self):
        super(CheckInstance, self).unposition()
        self.check.grid_forget()

    def set_prototype(self, prototype):
        super(CheckInstance, self).set_prototype(prototype)
        self.check_int.set(self.prototype.get_default())


class ChooseFileInstance(OptionInstance):
    def __init__(self, prototype, parent):
        super(ChooseFileInstance, self).__init__(parent)

        self.button_image = tk.PhotoImage(file=os.path.join(os.path.dirname(__file__), 'folder_icon.gif'))

        self.file_string = tk.StringVar(parent)
        self.frame = tk.Frame(parent)
        self.file_entry = tk.Entry(self.frame, textvariable=self.file_string, width=22)
        self.file_browse = tk.Button(self.frame, image=self.button_image, command=self.file_command)

        self.file_entry.pack(side=tk.LEFT, anchor=tk.W)
        self.file_browse.pack(side=tk.RIGHT, anchor=tk.E)

        self.set_prototype(prototype)

    def file_command(self):
        filename = tkFileDialog.asksaveasfilename(title="Choose a file")
        if len(filename):
            self.file_string.set(filename)

    def write(self, value):
        self.file_string.set(self.prototype.validate_and_convert(value))

    def read(self):
        return self.prototype.validate_and_convert(self.file_string.get())

    def position(self, row, column):
        super(ChooseFileInstance, self).position(row, column)
        self.frame.grid(row=row, column=column + 1)

    def unposition(self):
        super(ChooseFileInstance, self).unposition()
        self.frame.grid_forget()

    def set_prototype(self, prototype):
        super(ChooseFileInstance, self).set_prototype(prototype)
        self.file_string.set(self.prototype.get_default())
