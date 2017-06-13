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
The Tkinter dialogs
"""
import os
import ttk
from Tkinter import *
from abc import ABCMeta

from configobj import ConfigObj


class RowData:
    def __init__(self, widget=None, variable=None, label=None):
        self._widget = widget
        self._variable = variable
        self._label = label

    @property
    def widget(self):
        return self._widget

    @widget.setter
    def widget(self, value):
        self._widget = value

    @property
    def variable(self):
        return self._variable

    @variable.setter
    def variable(self, value):
        self._variable = value

    @property
    def label(self):
        return self._label

    @label.setter
    def label(self, value):
        self._label = value


class DialogCommon:
    # This ensures that:
    #  - This class cannot be instantiated
    #  - Subclasses implement methods decorated with @abstractmethod
    __metaclass__ = ABCMeta

    def __init__(self, config_obj):
        self._config_obj = config_obj
        self._root = None
        self._mainframe = None
        self._integer_validator = None
        self._float_validator = None
        self._widgets_used = {}

    def _initialise(self, title):
        self._root = Tk()
        self._root.title(title)

        self._mainframe = ttk.Frame(self._root, padding='3 3 12 12')
        self._mainframe.grid(column=0, row=0, sticky=(N, W, E, S))
        self._mainframe.columnconfigure(0, weight=1)
        self._mainframe.rowconfigure(0, weight=1)

    def _frame(self, parent=None, **kwargs):
        if parent is None:
            parent = self._mainframe
        return ttk.Frame(parent, **kwargs)

    def _entry(self, tag_name, parent=None, **kwargs):
        if parent is None:
            parent = self._mainframe
        string = StringVar()
        entry = ttk.Entry(parent, textvariable=string, **kwargs)
        if tag_name in self._config_obj:
            string.set(self._config_obj[tag_name])

        row_data = self._widgets_used.get(tag_name)
        if row_data is None:
            self._widgets_used[tag_name] = RowData(widget=entry, variable=string)
        else:
            row_data.widget = entry
            row_data.variable = string
        return entry

    def _label(self, tag_name, parent=None, **kwargs):
        if parent is None:
            parent = self._mainframe
        label = ttk.Label(parent, **kwargs)

        row_data = self._widgets_used.get(tag_name)
        if row_data is None:
            self._widgets_used[tag_name] = RowData(label=label)
        else:
            row_data.label = label
        return label

    def _checkbutton(self, tag_name, parent=None, **kwargs):
        if parent is None:
            parent = self._mainframe
        string = StringVar()
        checkbutton = ttk.Checkbutton(parent, variable=string, **kwargs)
        if tag_name in self._config_obj:
            string.set(self._config_obj[tag_name])

        row_data = self._widgets_used.get(tag_name)
        if row_data is None:
            self._widgets_used[tag_name] = RowData(widget=checkbutton, variable=string)
        else:
            row_data.widget = checkbutton
            row_data.variable = string
        return checkbutton

    def _combobox(self, tag_name, parent=None, **kwargs):
        if parent is None:
            parent = self._mainframe
        string = StringVar()
        combobox = ttk.Combobox(parent, textvariable=string, **kwargs)
        if tag_name in self._config_obj:
            string.set(self._config_obj[tag_name])

        row_data = self._widgets_used.get(tag_name)
        if row_data is None:
            self._widgets_used[tag_name] = RowData(widget=combobox, variable=string)
        else:
            row_data.widget = combobox
            row_data.variable = string
        return combobox

    @staticmethod
    def _validate_float(value_if_allowed, text):
        if text in '0123456789.-':
            try:
                float(value_if_allowed)
                return True
            except ValueError:
                return False
        else:
            return False

    @staticmethod
    def _validate_int(value_if_allowed, text):
        if text in '0123456789-':
            try:
                int(value_if_allowed)
                return True
            except ValueError:
                return False
        else:
            return False

    def _get_integer_validator(self):
        if self._integer_validator is None:
            self._integer_validator = (self._root.register(self._validate_int), '%P', '%S')

        return self._integer_validator

    def _get_float_validator(self):
        if self._float_validator is None:
            self._float_validator = (self._root.register(self._validate_float), '%P', '%S')

        return self._float_validator

    def _centre(self, window=None):
        if window is None:
            window = self._root

        window.update_idletasks()
        width = window.winfo_width()
        frm_width = window.winfo_rootx() - window.winfo_x()
        win_width = width + 2 * frm_width
        height = window.winfo_height()
        titlebar_height = window.winfo_rooty() - window.winfo_y()
        win_height = height + titlebar_height + frm_width
        x = window.winfo_screenwidth() // 2 - win_width // 2
        y = window.winfo_screenheight() // 2 - win_height // 2
        window.geometry('{}x{}+{}+{}'.format(width, height, x, y))
        window.deiconify()

    def ok_clicked(self):
        self._root.destroy()

        for key, value in self._widgets_used.iteritems():
            self._config_obj[key] = value.widget.get()

    def cancel_clicked(self):
        self._root.destroy()

    def _add_ok_cancel(self):
        button = ttk.Button(self._mainframe, text="Ok", command=self.ok_clicked)
        button.grid(column=3, row=1)
        button = ttk.Button(self._mainframe, text="Cancel", command=self.cancel_clicked)
        button.grid(column=3, row=2)

    def _start_up(self):
        self._centre()
        self._root.lift()
        self._root.attributes('-topmost', True)
        self._root.attributes('-topmost', False)
        self._root.mainloop()


class DialogUvsub(DialogCommon):
    def __init__(self, config_obj):
        DialogCommon.__init__(self, config_obj)

    def _new_selection(self):
        value_of_combo = self._widgets_used['run_type'].width.get()
        if value_of_combo == 'create':
            pass    # TODO
        elif value_of_combo == 'use':
            pass    # TODO
        elif value_of_combo == 'json':
            pass    # TODO

    def display(self):
        self._initialise("UV Subtraction")

        row = 1
        self._combobox(tag_name='run_type', state='readonly', values=('create', 'use', 'json')).grid(column=2, row=row)
        self._label(tag_name='run_type', text='Run type:', justify=RIGHT).grid(column=1, row=row)

        row += 1
        self._entry(tag_name='bucket_name').grid(column=2, row=row)
        self._label(tag_name='bucket_name', text='Bucket name:', justify=RIGHT).grid(column=1, row=row)

        row += 1
        self._entry(tag_name='volume').grid(column=2, row=row)
        self._label(tag_name='volume', text='DaLiuge Volume Name:', justify=RIGHT).grid(column=1, row=row)

        row += 1
        self._entry(tag_name='width', validatecommand=self._get_integer_validator()).grid(column=2, row=row)
        self._label(tag_name='width', text='Frequency width:', justify=RIGHT).grid(column=1, row=row)

        row += 1
        self._entry(tag_name='w_projection_planes', validatecommand=self._get_integer_validator()).grid(column=2, row=row)
        self._label(tag_name='w_projection_planes', text='Number W projection planes:', justify=RIGHT).grid(column=1, row=row)

        row += 1
        self._entry(tag_name='uvsub_directory_name').grid(column=2, row=row)
        self._label(tag_name='uvsub_directory_name', text='uvsub results directory in S3:', justify=RIGHT).grid(column=1, row=row)

        row += 1
        self._entry(tag_name='frequency_range').grid(column=2, row=row)
        self._label(tag_name='frequency_range', text='Frequency range:', justify=RIGHT).grid(column=1, row=row)

        row += 1
        self._entry(tag_name='ami').grid(column=2, row=row)
        self._label(tag_name='ami', text='AMI Id:', justify=RIGHT).grid(column=1, row=row)

        row += 1
        self._entry(tag_name='spot_price', validatecommand=self._get_float_validator()).grid(column=2, row=row)
        self._label(tag_name='spot_price', text='Spot price:', justify=RIGHT).grid(column=1, row=row)

        row += 1
        self._entry(tag_name='nodes', validatecommand=self._get_integer_validator()).grid(column=2, row=row)
        self._label(tag_name='nodes', text='Number of AWS nodes:', justify=RIGHT).grid(column=1, row=row)

        row += 1
        self._entry(tag_name='dim').grid(column=2, row=row)
        self._label(tag_name='dim', text='DIM IP Address:', justify=RIGHT).grid(column=1, row=row)

        row += 1
        self._checkbutton(tag_name='shutdown').grid(column=2, row=row)
        self._label(tag_name='shutdown', text='Add shutdown node:', justify=RIGHT).grid(column=1, row=row)

        row += 1
        self._checkbutton(tag_name='shutdown').grid(column=2, row=row)
        self._label(tag_name='shutdown', text='Calculate scan statistics:', justify=RIGHT).grid(column=1, row=row)

        row += 1
        self._checkbutton(tag_name='dump_json').grid(column=2, row=row)
        self._label(tag_name='dump_json', text='Dump the json:', justify=RIGHT).grid(column=1, row=row)

        self._run_type.widget.bind("<<ComboboxSelected>>", self._new_selection)

        self._add_ok_cancel()

        self._start_up()

if __name__ == '__main__':
    path_dirname, filename = os.path.split(__file__)
    config_file_name = '{0}/aws-chiles02.settings'.format(path_dirname)
    if os.path.exists(config_file_name):
        config = ConfigObj(config_file_name)
    else:
        config = ConfigObj()
    config.filename = config_file_name

    dialog = DialogUvsub(config)
    dialog.display()
