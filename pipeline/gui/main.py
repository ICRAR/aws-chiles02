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
GUI Main
"""
import Tkinter as tk
from api import NullAPI
from save_impl import ChilesGUIConfig
from data import DataAccess
from functools import partial
from utils import pluralise
from cache import Cache
from option_def import get_options, task_options, action_options

from validation import ValidationException

"""
Wizard with 3 pages for configuring options
Page1: Select Task (clean graph, imageconcat, jpeg2000, mstransform, uvsub)
       And action (Create, Use, Save JSON)
Page2: Configure Task (Shows options specific to task)
Page3: Confirm (runs the task)
"""


class Wizard:
    """
    The Wizard class manages a set of wizard pages and provides a back and forward navigation between the pages.
    Each wizard page contained within a tk.Frame, and the back and forward buttons are placed at the bottom of the screen.
    """
    def __init__(self, root):
        """
        :param pages: List of pages, in order, that the wizard will sequentially go through
        """
        self.pages = []
        self.current_page = None
        self.current_page_index = 0
        self.on_submit = None
        self.frame = tk.Frame(root)
        self.frame.pack()

        navigation = tk.Frame(self.frame)
        navigation.pack(side=tk.BOTTOM)

        self.previous_button = tk.Button(navigation, text="Previous", command=self.previous_page, width=5)
        self.previous_button.pack(side=tk.LEFT)

        self.next_button = tk.Button(navigation, text="Next", command=self.next_page, width=5)
        self.next_button.pack(side=tk.RIGHT)

    def __len__(self):
        """
        Number of pages in the wizard
        :return:
        """
        return len(self.pages)

    def set_on_submit(self, callback):
        """
        Set the function to be called when the wizard has gone past the last page
        :param function:
        :return:
        """
        self.on_submit = callback

    def get_page(self, index):
        """
        Get a page from the wizard
        :param index:
        :return:
        """
        return self.pages[index]

    def add_page(self, page):
        """
        Add a page to the wizard
        :return:
        """
        page.frame = tk.Frame(self.frame)
        self.pages.append(page)

        if len(self) == 1:
            self.goto_page(0)
        else:
            self._update_buttons()

    def next_page(self):
        """
        Go to the next page in the wizard
        :return:
        """
        self.goto_page(self.current_page_index + 1)

    def previous_page(self):
        """
        Go to the previous page in the wizard
        :return:
        """
        self.goto_page(self.current_page_index - 1)

    def goto_page(self, index):
        """
        Go to the specified page in the wizard
        :param index:
        :return:
        """

        if index < 0 or index > len(self):
            return

        if index == len(self):
            if self.on_submit is not None:
                self.on_submit(self)
            return

        new_page = self.pages[index]
        old_page = self.current_page

        if old_page:
            old_page.leave(new_page)
            old_page.frame.pack_forget()

        self.current_page = new_page
        self.current_page_index = index

        new_page.frame.pack(side=tk.TOP)
        new_page.enter(old_page)

        self._update_buttons()

    def _update_buttons(self):
        """
        Updates the state of the wizard buttons.
        :return:
        """
        self.previous_button.config(state=tk.DISABLED if self.current_page_index == 0 else tk.NORMAL)
        self.next_button.config(text="Submit" if self.current_page_index == (len(self) - 1) else "Next")


class WizardPage(object):

    def __init__(self, wizard):
        super(WizardPage, self).__init__()
        self.wizard = wizard
        self.frame = None
        wizard.add_page(self)

    @staticmethod
    def clear_children(widget):
        """
        Clears all children from this page
        :return:
        """
        for widget in widget.winfo_children():
            widget.destroy()

    def enter(self, from_page):
        """
        Called when this page becomes active.
        :param from_page:
        :return:
        """
        pass

    def leave(self, to_page):
        """
        Called when this page becomes inactive
        :param to_page:
        :return:
        """
        pass


class SelectTaskPage(WizardPage):

    def __init__(self, *args):
        super(SelectTaskPage, self).__init__(*args)
        self.task_option = tk.StringVar(value=task_options[0])
        self.action_option = tk.StringVar(value=action_options[0])

        label = tk.Label(self.frame, text="Select a task and action")
        label.pack(side=tk.TOP)

        select_task = tk.OptionMenu(self.frame, self.task_option, *task_options)
        select_task.pack(side=tk.TOP)
        select_task.config(width=15)

        select_action = tk.OptionMenu(self.frame, self.action_option, *action_options)
        select_action.pack(side=tk.TOP)
        select_action.config(width=15)

    def enter(self, from_page):
        self.frame.winfo_toplevel().title("Chiles GUI")

    def leave(self, to_page):
        self.frame.winfo_toplevel().title("{0} > {1}".format(self.task_option.get(), self.action_option.get()))


class Configure(WizardPage):

    def __init__(self, select_task_page, data_access, save_load, *args):
        super(Configure, self).__init__(*args)
        self.select_task_page = select_task_page
        self.data_access = data_access
        self.save_load = save_load

        self.config_frame = None

        self.cache = {
            "Input": Cache(),
            "Select": Cache(),
            "Check": Cache(),
            "ChooseFile": Cache()
        }

        self.option_prototypes = []
        self.option_instances = []
        self.defaults = {}
        self.initial = {}

        self.task = None
        self.action = None

        # Title text
        label = tk.Label(self.frame, text="Configure")
        label.pack(side=tk.TOP)

        # Frame to hold menu buttons
        menu = tk.Frame(self.frame)
        menu.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        # Reset to default values button
        button = tk.Button(menu, text="Defaults", command=self.restore_defaults)
        button.pack(side=tk.LEFT, anchor=tk.W, padx=5)

        # Frame to hold config options
        self.config_frame = tk.Frame(self.frame)
        self.config_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=5)

    def enter(self, from_page):
        task = self.select_task_page.task_option.get()
        action = self.select_task_page.action_option.get()

        if self.task == task and self.action == action:
            return  # Nothing has changed, so there's nothing to do

        # Store the new task and action
        self.task = task
        self.action = action

        # Clear data access, and unposition all instances.
        self.data_access.clear()
        for cache in self.cache.itervalues():
            cache.return_all(lambda oi: oi.unposition())  # Unposition all option instances

        # Get the options we need to show, based on the task and action.
        self.option_prototypes = get_options(task, action)
        self.defaults = {option.id: option.default for option in self.option_prototypes}

        # Create the options for this frame from the options prototypes we just obtained
        autosaves = self.save_load.load()
        for index, option_prototype in enumerate(self.option_prototypes):
            # Get the saves list for this option, or an empty list if there are none
            try:
                history = list(reversed(autosaves[option_prototype.id])) # Reverse so the newest is at the front, oldest at the back
            except KeyError:
                history = []

            # Get either a pre-existing instance matching this prototype, or create a new one
            option_instance = self.cache[option_prototype.option_type].get(lambda: option_prototype.create(self.config_frame))
            # Set the parameters for the instance
            option_instance.set_prototype(option_prototype)
            option_instance.position(index)
            option_instance.set_history(history)

            # Add the data read / write functions for this instance
            self.data_access.add_write(option_prototype.id, option_instance.write)
            self.data_access.add_read(option_prototype.id, option_instance.read)

            # Store a list of all active instances

        self.initial = self.data_access.read()

    def restore_defaults(self):
        """
        Restore all config items to their default values
        :return:
        """
        self.data_access.write(**self.defaults)


class Confirm(WizardPage):

    class LabelValue:
        def __init__(self, frame):
            self.label_string = tk.StringVar()
            self.value_string = tk.StringVar()

            self.label = tk.Label(frame, textvariable=self.label_string, justify=tk.LEFT) # Label for field, with data name
            self.value = tk.Label(frame, textvariable=self.value_string) # Value for field

        def set(self, label, value):
            """
            Sets the label and value
            :param label: New label string
            :param value: New value string
            :return:
            """
            self.label_string.set(label + ":")
            self.value_string.set(value)

        def position(self, index):
            """
            Positions the label and value at the desired index
            :param index: The index to position at
            :return:
            """
            self.label.grid(row=index, column=1, sticky=tk.W)
            self.value.grid(row=index, column=2)

        def unposition(self):
            """
            Unpositions the label, hiding it.
            :return:
            """
            self.label.grid_forget()
            self.value.grid_forget()

    def __init__(self, data_access, *args):
        super(Confirm, self).__init__(*args)
        self.data_access = data_access
        self.data = None
        self.config_frame = None

        self.label_cache = Cache()

        # Title text
        label = tk.Label(self.frame, text="Confirm")
        label.pack(side=tk.TOP)

        # Frame to hold all final config options
        self.config_frame = tk.Frame(self.frame)
        self.config_frame.pack(side=tk.BOTTOM, fill=tk.X)

    def enter(self, from_page):
        # Clear the entire config frame to make way for the new fields
        data = self.data_access.read()

        if data == self.data:
            return  # Nothing new to display

        # Clear everything and store the new data
        self.label_cache.return_all(lambda l: l.unposition())  # Unposition all labels
        self.data = data

        # Read final data, then create fields to display it
        for index, (k, v) in enumerate(data.iteritems()):
            label = self.label_cache.get(lambda: self.LabelValue(self.config_frame))
            label.set(k, v)
            label.position(index)


class Action:
    def __init__(self, root, name, command, config_items):
        self.root = root
        self.frame = None
        self.save_load = ChilesGUIConfig("./{0}".format(name))
        self.data_access = DataAccess()
        self.autoload = None
        self.error_label = tk.StringVar()
        self.history_menu_items = 0

        self.defaults = {item.id: item.default for item in config_items}
        self.read_all_items = [item.id for item in config_items]

        self.build_frame(name, command, config_items)

        self.root.protocol("WM_DELETE_WINDOW", self.close_window)

        try:
            autoload = self.save_load.autoload()
            if autoload is not None:
                print "Autoloading..."
                self.data_access.write(**autoload)
                self.autoload = self.data_access.read(*self.read_all_items)  # Store the initial state so we can compare to it later to see if things changed and we need to autosave
                self.error_label.set("Loaded previous configuration")
        except ValidationException as e:
            self.show_data_error(e)
        except Exception as e:
            self.error_label.set("Failed to autoload last configuration correctly.")
            print e

    def get_all_autosaves(self):
        return [self.save_load.load(f[0]) for f in self.save_load.autosave_list(absolute=True)]

    def build_frame(self, name, command, items):

        toolbar_bottom = tk.Frame(self.root, borderwidth=1, relief=tk.GROOVE)

        label = tk.Label(toolbar_bottom, textvariable=self.error_label)
        label.pack(side=tk.BOTTOM, fill=tk.X, expand=True)

        toolbar_bottom.pack(side=tk.BOTTOM, fill=tk.X)

        # Put the run button at the bottom
        b = tk.Button(self.root, text=name, width=15, command=partial(self.command, command))
        b.pack(side=tk.BOTTOM)

        toolbar_top = tk.Menu(self.root)
        toolbar_top.add_command(label="Default", command=self.new)
        self.root.config(menu=toolbar_top)

        history_menu = tk.Menu(toolbar_top, tearoff=0)
        history_menu.config(postcommand=partial(self.build_history_menu, history_menu))

        toolbar_top.add_cascade(label="History", menu=history_menu)

        self.frame = tk.Frame(self.root)
        self.frame.pack(side=tk.TOP, fill=tk.X, expand=True)

        config_frame = tk.Frame(self.frame)
        config_frame.pack(side=tk.BOTTOM, fill=tk.X)

        autosaves = self.get_all_autosaves()

        for index, item in enumerate(items):
            item.create(config_frame, index, self.data_access, [s[item.id] for s in autosaves])

    def load_filename(self, filename):
        try:
            config = self.save_load.load(filename)
            self.data_access.write(**config)
            self.error_label.set("")

        except ValidationException as e:
            self.show_data_error(e)

        except Exception as e:
            self.error_label.set("Failed to load config file from: {0}".format(filename))
            print e.message

    def show_data_error(self, e):
        # Set the text in the bottom bar to match this
        text = "The "
        for index, error in enumerate(e.exceptions):
            text += error.field
            if index == len(e.exceptions) - 2:
                text += ", and " if len(e.exceptions) > 2 else " and "
            elif index != len(e.exceptions) - 1:
                text += ", "

        text += " {0} invalid".format(pluralise(len(e.exceptions), "field"))
        self.error_label.set(text)

    def new(self):
        # Clear everything
        self.data_access.write(**self.defaults)
        self.error_label.set("")

    def build_history_menu(self, menu):
        # Clear it first
        if self.history_menu_items:
            menu.delete(0, self.history_menu_items)
            self.history_menu_items = 0

        autosaves = self.save_load.autosave_list(absolute=True)

        for index, save in enumerate(autosaves):
            text = "Autosave {0}".format(save[1])
            if index == 0:
                text += " (latest)"
            menu.add_command(label=text, command=partial(self.load_filename, save[0]))
            self.history_menu_items += 1

        if self.history_menu_items == 0:
            menu.add_command(label="None")
            self.history_menu_items += 1

    def close_window(self):
        try:
            data = self.data_access.read(*self.read_all_items)
            if data != self.autoload:
                print "Autosaving..."
                self.save_load.autosave(data)
        except Exception as e:
            print e
        finally:
            self.root.destroy()

    def command(self, command):
        """
        Executes a command from the GUI.
        If there's an issue with the data items provided, the bottom panel of the UI will be
        set with an error message describing what's wrong with the current input.
        :param what: The command name to execute
        :param data_items: The config options from the gui to convert to a dictionary, then send to the command.
        """
        try:
            data = self.data_access.read(*self.read_all_items)
            self.error_label.set("")
            command(data)

        except ValidationException as e:
            self.show_data_error(e)


class ChilesGUI:
    """
    The main Chiles GUI class.
    """

    def __init__(self, root, api):
        """
        Initialise the GUI and create all of the window elements.
        :param root: The TKINTER root frame.
        :param api: The API for interacting with the rest of chiles.
        :param save_load: The system to handle saving and loading of config files.
        """
        self.root = root
        self.api = api

        self.root.title("aws-chiles02")

        label = tk.Label(self.root, text="Select an Action")
        label.pack(side=tk.TOP)

        tk.Button(self.root, text="Use", command=self.open_use, width=15).pack(side=tk.TOP)
        tk.Button(self.root, text="JSON", command=self.open_json, width=15).pack(side=tk.TOP)
        tk.Button(self.root, text="Create", command=self.open_create, width=15).pack(side=tk.TOP)

    def position(self, window):
        dx = 0
        dy = 0

        x = self.root.winfo_x()
        y = self.root.winfo_y()
        window.geometry("+%d+%d" % (x + dx, y + dy))

    def open_use(self):
        """
        Builds the main config frame for the GUI
        """
        use = tk.Toplevel()
        use.title("aws-chiles02 - Use")
        use.grab_set()
        Action(use, "Use", self.api.use, common_items + use_items)
        self.position(use)

    def open_json(self):
        json = tk.Toplevel()
        json.title("aws-chiles02 - JSON")
        json.grab_set()
        Action(json, "JSON", self.api.generate_json, common_items + json_items)
        self.position(json)

    def open_create(self):
        create = tk.Toplevel()
        create.title("aws-chiles02 - Create")
        create.grab_set()
        Action(create, "Create", self.api.create, common_items + create_items)
        self.position(create)


def run_gui(api):
    """
    Runs the GUI for chiles
    :param api: The API for the rest of the program.
    :param save: The system to handle saving and loading of config files.
    :return:
    """
    root = tk.Tk()
    root.resizable(0, 0)

    #gui = ChilesGUI(root, api)
    data_access = DataAccess()
    chiles_gui_config = ChilesGUIConfig()

    wizard = Wizard(root)
    page1 = SelectTaskPage(wizard)
    page2 = Configure(page1, data_access, chiles_gui_config, wizard)
    page3 = Confirm(data_access, wizard)

    def submit(wizard):
        data = data_access.read()
        chiles_gui_config.save(data)
        print data

    wizard.set_on_submit(submit)

    ws = root.winfo_screenwidth()  # width of the screen
    hs = root.winfo_screenheight()  # height of the screen

    root.geometry('+%d+%d' % (ws * 0.5, hs * 0.5))

    root.mainloop()


if __name__ == "__main__":
    run_gui(NullAPI(), )
