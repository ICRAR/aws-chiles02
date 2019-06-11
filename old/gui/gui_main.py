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
GUI Main - Run this file to run the Chiles GUI.
"""
import Tkinter as tk
import logging
import argparse

from old.gui.gui_api import NullAPI, ChilesAPI, api_command
from old.gui.save import ChilesGUIConfig
from old.gui.data import DataAccess
from old.gui.cache import Cache
from old.gui.wizard import Wizard, WizardPage
from old.gui.option_definitions import get_options, task_options, action_options
from old.gui.validation import ValidationException
"""
Wizard with 3 pages for configuring options
Page1: Select Task (clean graph, imageconcat, jpeg2000, mstransform, uvsub)
       And action (Create, Use, Save JSON)
Page2: Configure Task (Shows options specific to task)
Page3: Confirm (runs the task)
"""

FRAME_SIZE = 550
label_font = ("Helvetica", 15, "bold italic")


def resize_single_spacer(spacer, parent_frame):
    """
    Try and ensure the frame remains a similar height to the others by adding padding to the bottom
    :param spacer:
    :param parent_frame:
    :return:
    """

    # This code is ridiculous. For some reason, even if I call pack_propagate(False) on the spacer, it resizes
    # itself and ignores my resize requests. The only way to size it properly is to completely unpack the damn thing,
    # force a window update, work out the spacer size needed, then re-pack the spacer.
    spacer.pack_forget()
    parent_frame.update()
    spacer.config(height=max(FRAME_SIZE - parent_frame.winfo_height(), 0))
    spacer.pack(side=tk.TOP)


class SelectTaskPage(WizardPage):
    page_name = "SelectTask"

    def __init__(self):
        super(SelectTaskPage, self).__init__()
        self.task_option = tk.StringVar(value=task_options[0])
        self.action_option = tk.StringVar(value=action_options[0])
        self.space1 = None
        self.space2 = None

    def create(self):
        label = tk.Label(self.frame, text="Select a task and action", font=label_font, relief=tk.GROOVE)
        label.pack(side=tk.TOP, fill=tk.X)

        self.space1 = tk.Frame(self.frame, width=500)
        self.space2 = tk.Frame(self.frame, width=500)

        # As this page is static, I've foregone any dynamic spacers in favour of a precalculated spacer height.
        self.space1.pack(side=tk.TOP)
        self.space1.config(height=214)  # Precalculated to ensure height of 550

        select_task_frame = tk.Frame(self.frame)
        select_task_frame.pack(side=tk.TOP, pady=5)
        select_task_label = tk.Label(select_task_frame, text="Task:", width=7, justify=tk.LEFT, anchor=tk.W)
        select_task_label.pack(side=tk.LEFT)
        select_task = tk.OptionMenu(select_task_frame, self.task_option, *task_options)
        select_task.pack(side=tk.RIGHT)
        select_task.config(width=15)

        select_action_frame = tk.Frame(self.frame)
        select_action_frame.pack(side=tk.TOP, pady=5)
        select_action_label = tk.Label(select_action_frame, text="Action:", width=7, justify=tk.LEFT, anchor=tk.W)
        select_action_label.pack(side=tk.LEFT)
        select_action = tk.OptionMenu(select_action_frame, self.action_option, *action_options)
        select_action.pack(side=tk.RIGHT)
        select_action.config(width=15)

        self.space2.pack(side=tk.BOTTOM, pady=5)
        self.space2.config(height=215)  # Precalculated to ensure height of 550

        self.frame.update()

    def enter(self, from_page):
        self.frame.winfo_toplevel().title("Chiles GUI")

    def leave(self, to_page):
        self.frame.winfo_toplevel().title("{0} > {1}".format(self.task_option.get(), self.action_option.get()))
        return True


class ConfigurePage(WizardPage):
    page_name = "Configure"

    def __init__(self, select_task_page, data_access, save_load):
        super(ConfigurePage, self).__init__()
        self.select_task_page = select_task_page
        self.data_access = data_access
        self.save_load = save_load

        self.config_frame = None
        self.config_spacer_bottom = None

        self.cache = {
            "Input": Cache(),
            "Text Field": Cache(),
            "Select": Cache(),
            "Check": Cache(),
            "ChooseFile": Cache()
        }

        self.option_prototypes = []
        self.option_instances = []
        self.defaults = {}

        self.task = None
        self.action = None

    def create(self):
        # Title text
        label = tk.Label(self.frame, text="Configure", font=label_font, relief=tk.GROOVE)
        label.pack(side=tk.TOP, fill=tk.X)

        # Frame to hold menu buttons
        menu = tk.Frame(self.frame)
        menu.pack(side=tk.TOP, fill=tk.BOTH, expand=True, pady=5)

        # Reset to default values button
        button = tk.Button(menu, text="Set to Defaults", command=self.restore_defaults)
        button.pack(side=tk.LEFT, anchor=tk.W, padx=5)

        # Spacer below config options
        self.config_spacer_bottom = tk.Frame(self.frame)
        self.config_spacer_bottom.pack_propagate(False)
        self.config_spacer_bottom.pack(side=tk.BOTTOM)

        # Frame to hold config options
        self.config_frame = tk.Frame(self.frame)
        self.config_frame.pack(side=tk.TOP, fill=tk.X, padx=5)

        tk.Frame(self.config_frame).grid()
        tk.Frame(self.frame).pack()

    def enter(self, from_page):
        task = self.select_task_page.task_option.get()
        action = self.select_task_page.action_option.get()

        if self.task == task and self.action == action:
            return  # Nothing has changed, so there's nothing to do

        # Store the new task and action
        self.task = task
        self.action = action

        # Clear data access fields as we'll be rebuilding them in a moment
        self.data_access.clear()

        # Unposition all option instances. This removes them from the frame entirely.
        for cache in self.cache.itervalues():
            cache.return_all(lambda oi: oi.unposition())

        # Get the options we need to show, based on the task and action.
        self.option_prototypes = get_options(task, action)
        self.defaults = {option.id: option.default for option in self.option_prototypes}

        # We now have a list of option prototypes, and potentially a set of old cached option instances.
        # We now iterate over the prototypes we need to display, and reused cached option instances if they exist.
        # This is done to keep creations of new tkinter objects at runtime to a minimum. New instances are only
        # created if there are no cached instances remaining that have the same prototype type as the new instance.
        autosaves = self.save_load.load()
        column = 0
        for index, option_prototype in enumerate(self.option_prototypes):
            if index % 15 == 0:
                column += 3

            index = index % 15
            # Get the saves list for this option, or an empty list if there are none
            try:
                history = list(reversed(autosaves[option_prototype.id]))  # Reverse so the newest is at the front, oldest at the back
            except KeyError:
                history = []

            # Get either a pre-existing instance matching this prototype, or create a new one
            option_instance = self.cache[option_prototype.option_type].get(lambda: option_prototype.create(self.config_frame))

            # Add the data read / write functions for this instance
            self.data_access.add_write(option_prototype.id, option_instance.write)
            self.data_access.add_read(option_prototype.id, option_instance.read)

            # Set the parameters for the instance
            option_instance.set_prototype(option_prototype)
            option_instance.position(index, column)
            option_instance.set_history(history)

        # try and ensure the frame remains a similar height to the others by adding padding to the bottom
        resize_single_spacer(self.config_spacer_bottom, self.frame)

    def leave(self, to_page):
        if to_page.page_name == "Confirm":
            try:
                self.data_access.read()
            except ValidationException as e:
                print(e.message)
                return False

        return True

    def restore_defaults(self):
        """
        Restore all config items to their default values
        :return:
        """
        self.data_access.write(**self.defaults)

    def force_update(self):
        """
        Forces the rebuild of all items on this page when the user returns to it.
        This is used to force the history to update after the user presses submit on the wizard
        :return:
        """
        self.task = None
        self.action = None


class ConfirmPage(WizardPage):
    page_name = "Confirm"

    class LabelValue:
        def __init__(self, frame):
            self.label_string = tk.StringVar()
            self.value_string = tk.StringVar()

            self.label = tk.Label(frame, textvariable=self.label_string, justify=tk.LEFT, anchor=tk.W)  # Label for field, with data name
            self.value = tk.Label(frame, textvariable=self.value_string, width=10, justify=tk.RIGHT, anchor=tk.E)  # Value for field

        def set(self, label, value):
            """
            Sets the label and value
            :param label: New label string
            :param value: New value string
            :return:
            """
            self.label_string.set(label + ":")
            self.value_string.set(value)

        def position(self, index, column):
            """
            Positions the label and value at the desired index
            :param index: The index to position at
            :return:
            """
            self.label.grid(row=index, column=column, sticky=tk.W)
            self.value.grid(row=index, column=column + 1, sticky=tk.E)

        def unposition(self):
            """
            Unpositions the label, hiding it.
            :return:
            """
            self.label.grid_forget()
            self.value.grid_forget()

    def __init__(self, data_access):
        super(ConfirmPage, self).__init__()
        self.data_access = data_access
        self.data = None
        self.config_frame = None
        self.config_spacer_bottom = None

        self.label_cache = Cache()

    def create(self):
        # Title text
        label = tk.Label(self.frame, text="Confirm", font=label_font, relief=tk.GROOVE)
        label.pack(side=tk.TOP, fill=tk.X, expand=True)

        # Spacer below config options
        self.config_spacer_bottom = tk.Frame(self.frame)
        self.config_spacer_bottom.pack(side=tk.BOTTOM, fill=tk.X, expand=True)

        # Frame to hold all final config options
        self.config_frame = tk.Frame(self.frame)
        self.config_frame.pack(side=tk.TOP, fill=tk.X, expand=True)

    def enter(self, from_page):
        # Clear the entire config frame to make way for the new fields
        data = self.data_access.read()

        if data == self.data:
            return  # Nothing new to display

        # Clear everything and store the new data
        self.label_cache.return_all(lambda l: l.unposition())  # Unposition all labels
        self.data = data

        # Read final data, then create fields to display it
        count = 0
        for index, (k, v) in enumerate(data.iteritems()):
            label = self.label_cache.get(lambda: self.LabelValue(self.config_frame))
            label.set(k, str(v))
            label.position(index, 0)
            count += 1

        resize_single_spacer(self.config_spacer_bottom, self.frame)


class ChilesGUI:

    def __init__(self, root, api):
        """

        :param root:
        :param api:
        """
        self.root = root
        self.api = api

        self.data_access = DataAccess()
        self.gui_config = ChilesGUIConfig()

        self.wizard = Wizard(root)
        self.wizard.frame.pack()

        self.select_task_page = SelectTaskPage()
        self.configure_page = ConfigurePage(self.select_task_page, self.data_access, self.gui_config)
        self.confirm_page = ConfirmPage(self.data_access)

        self.wizard.add_page(self.select_task_page)
        self.wizard.add_page(self.configure_page)
        self.wizard.add_page(self.confirm_page)

        self.wizard.set_on_submit(self.wizard_submit)

    def save(self):
        data = self.data_access.read()
        if len(data):
            self.gui_config.save(data)
        return data

    def wizard_submit(self, wizard):
        data = self.save()  # On wizard submit, don't use defaults to save because we want all the values that were just used to be saved.
        self.configure_page.force_update()
        self.root.destroy()
        api_command(self.api, self.select_task_page.task_option.get(), self.select_task_page.action_option.get(), data)


def run_gui(api):
    """
    Runs the GUI for chiles
    :param api: The API for the rest of the program.
    :param save: The system to handle saving and loading of config files.
    :return:
    """
    root = tk.Tk()
    root.resizable(0, 0)

    gui = ChilesGUI(root, api)

    ws = root.winfo_screenwidth()  # width of the screen
    hs = root.winfo_screenheight()  # height of the screen

    # Try and ensure the window is created at a reasonable position within the user's screen.
    root.geometry('+%d+%d' % (ws * 0.25, hs * 0.25))

    def close_window():
        try:
            print("Autosaving...")
            gui.save()
        except Exception as e:
            print(e)
        finally:
            root.destroy()

    root.protocol("WM_DELETE_WINDOW", close_window)

    root.mainloop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--null_api", action="store_true", help="Use the Null API for testing")
    args = vars(parser.parse_args())
    run_gui(NullAPI() if args.get('null_api', False) else ChilesAPI())
