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
import ttk
import tkFileDialog
from api import NullAPI
from save_impl import ChilesGUIConfig
from data import DataAccess
from functools import partial
from utils import pluralise

from validation import Int, Float, String, SelectList, ValidationException
from option import Input, Select, Check, ChooseFile


class ChilesGUI:
    """
    The main Chiles GUI class.
    """
    weighting_uv_options = [
        "Briggs",
        "Uniform",
        "Natural"
    ]

    clean_tclean_options = [
        "clean",
        "tclean"
    ]

    casa_version_options = [
        "4.7",
        "5.1"
    ]

    log_level_options = [
        "v",
        "vv",
        "vvv"
    ]

    option_bucket = Input("bucket", "Bucket Name", String(), default='13b-266')
    option_frequency = Input("frequency", "Frequency Width", Float(), default=4)
    option_iterations = Input("iterations", "Iterations", Int(), default=1)
    option_arc_seconds = Input("arc_seconds", "Arc Seconds", Float(), default=2)
    option_w_projection_planes = Input("w_projection_planes", "W Projection Planes", Int(), default=24)
    option_clean_weighting_uv = Select("clean_weighting_uv", "Clean Weighting UV", weighting_uv_options, default='Briggs')
    option_clean_robust = Input("clean_robust", "Clean Robust Value", Float(), default=0.8)
    option_image_size = Input("image_size", "Image Size", Int(), default=4096)
    option_clean_channel_average = Input("clean_channel_average", "Input channels to average", Int(), default=1)
    option_region_file = Input("region_file", "Region file", String())
    option_only_image = Check("only_image", "Only copy image to S3")
    option_shutdown = Check("shutdown", "Add shutdown node", default=True)
    option_build_fits = Check("build_fits", "Build FITS for JPEG2000")
    option_fits_directory_name = Input("fits_directory_name", "FITS directory", String())
    option_uvsub_directory_name = Input("uvsub_directory_name", "UVSUB directory", String())
    option_clean_directory_name = Input("clean_directory_name", "Clean directory", String())
    option_produce_qa = Check("produce_qa", "Produce QA products")
    option_clean_tclean = Select("clean_tclean", "Clean or TClean", clean_tclean_options, default="clean")
    option_use_bash = Check("use_bash", "Run CASA in Bash")
    option_casa_version = Select("casa_version", "CASA version", casa_version_options, default='5.1')
    option_volume = Input("volume", "Host directory for Docker", String())
    option_frequency_range = Input("frequency_range", "Frequeny Ranges", String())
    option_run_note_clean = Input("run_note_clean", "Note", String(), default='No note')
    option_ami = Input("ami", "AMI ID", String())
    option_spot_price_i3_4xlarge = Input("spot_price_i3_4xlarge", "Spot Price for i3.4xlarge", String())
    option_frequencies_per_node = Input("frequencies_per_node", "Frequencies per node", Int(), default=1)
    option_log_level = Select("log_level", "Log level", log_level_options, default='vvv')
    option_dim = Input("dim", "Data island manager IP", String())
    option_nodes = Input("nodes", "Node count", Int(), default=1)
    option_json = ChooseFile("json", "JSON output path", default='/tmp/json_clean.txt')

    common_items = [
        option_bucket,
        option_frequency,
        option_iterations,
        option_arc_seconds,
        option_w_projection_planes,
        option_clean_weighting_uv,
        option_clean_robust,
        option_image_size,
        option_clean_channel_average,
        option_region_file,
        option_only_image,
        option_shutdown,
        option_build_fits,
        option_fits_directory_name,
        option_uvsub_directory_name,
        option_clean_directory_name,
        option_produce_qa,
        option_clean_tclean,
        option_use_bash,
        option_casa_version,
        option_volume,
        option_frequency_range,
        option_run_note_clean
    ]

    use_items = [
        option_dim
    ]

    create_items = [
        option_ami,
        option_spot_price_i3_4xlarge,
        option_frequencies_per_node,
        option_log_level
    ]

    json_items = [
        option_nodes,
        option_json
    ]

    all_items = common_items + use_items + create_items + json_items

    read_all_items = [item.id for item in all_items]
    write_all_defaults = {item.id: item.default for item in all_items}

    def __init__(self, root, api):
        """
        Initialise the GUI and create all of the window elements.
        :param root: The TKINTER root frame.
        :param api: The API for interacting with the rest of chiles.
        :param save_load: The system to handle saving and loading of config files.
        """
        self.root = root
        self.api = api
        self.save_load = ChilesGUIConfig(".")
        self.data_access = DataAccess()

        self.toolbar_top = None
        self.toolbar_bottom = None
        self.main = None
        self.history_menu_items = 0

        self.error_label = tk.StringVar()

        self.build_toolbars()
        self.build_main()

        self.autoload = None

        root.protocol("WM_DELETE_WINDOW", self.close_window)

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

    def command(self, what, data_items):
        """
        Executes a command from the GUI.
        If there's an issue with the data items provided, the bottom panel of the UI will be
        set with an error message describing what's wrong with the current input.
        :param what: The command name to execute
        :param data_items: The config options from the gui to convert to a dictionary, then send to the command.
        """
        try:
            data = self.data_access.read(*[item.id for item in data_items])

            self.error_label.set("")

            f = getattr(self.api, what, None)

            if f is not None:
                f(data)

        except ValidationException as e:
            self.show_data_error(e)

    def new(self):
        # Clear everything
        self.data_access.write(**self.write_all_defaults)
        self.error_label.set("")

    def save(self):
        """
        Save the current GUI state to a file.
        :return:
        """
        try:
            data = self.data_access.read(*self.read_all_items)
            filename = tkFileDialog.asksaveasfilename(title="Choose a file to save to")

            if len(filename) > 0:
                self.save_load.save(filename, data)

        except ValidationException as e:
            self.show_data_error(e)

        except Exception as e:
            self.error_label.set("Failed to write config file to: {0}".format(filename))
            print e.message

    def load(self):
        """
        Load the current GUI state from a file
        :return:
        """
        filename = tkFileDialog.askopenfilename(title="Choose a file to load from")

        if len(filename) > 0:
            self.load_filename(filename)

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

    def build_history_menu(self, menu):
        # Clear it first
        if self.history_menu_items:
            menu.delete(0, self.history_menu_items)
            self.history_menu_items = 0

        autosaves = self.save_load.autosave_list(absolute=True)

        for index, save in enumerate(autosaves):
            text = "Autosave {0}".format(index)
            if index == 0:
                text += " (latest)"
            menu.add_command(label=text, command=partial(self.load_filename, save[0]))
            self.history_menu_items += 1

        if self.history_menu_items == 0:
            menu.add_command(label="None")
            self.history_menu_items += 1

    def build_toolbars(self):
        """
        Builds the two toolbars at the bottom and top of the GUI
        """
        self.toolbar_bottom = tk.Frame(self.root, borderwidth=1, relief=tk.GROOVE)

        label = tk.Label(self.toolbar_bottom, textvariable=self.error_label)
        label.pack(side=tk.BOTTOM, fill=tk.X, expand=True)

        self.toolbar_bottom.pack(side=tk.BOTTOM, fill=tk.X)

        self.toolbar_top = tk.Menu(self.root)
        self.toolbar_top.add_command(label="Default", command=self.new)
        self.toolbar_top.add_command(label="Save", command=self.save)
        self.toolbar_top.add_command(label="Load", command=self.load)
        self.root.config(menu=self.toolbar_top)

        history_menu = tk.Menu(self.toolbar_top, tearoff=0)
        history_menu.config(postcommand=partial(self.build_history_menu, history_menu))

        self.toolbar_top.add_cascade(label="History", menu=history_menu)

    def build_main(self):
        """
        Builds the main config frame for the GUI
        """
        self.main = tk.Frame(self.root)
        self.main.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        common_config = tk.Frame(self.main)
        common_config.pack(side=tk.LEFT, fill=tk.BOTH)

        tabbed_config = tk.Frame(self.main)
        tabbed_config.pack(side=tk.RIGHT, fill=tk.BOTH)

        label = tk.Label(common_config, text="Common Configuration", borderwidth=1, relief=tk.GROOVE)
        label.pack(side=tk.TOP, fill=tk.X)

        common_config_frame = tk.Frame(common_config)
        common_config_frame.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=True)
        self.build_common(common_config_frame)

        label = tk.Label(tabbed_config, text="Command Configuration", borderwidth=1, relief=tk.GROOVE)
        label.pack(side=tk.TOP, fill=tk.X)

        tabbed_config_frame = ttk.Notebook(tabbed_config)
        tabbed_config_frame.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=True)

        use = ttk.Frame(tabbed_config_frame)
        self.build_use(use)
        tabbed_config_frame.add(use, text="Use")

        create = ttk.Frame(tabbed_config_frame)
        self.build_create(create)
        tabbed_config_frame.add(create, text="Create")

        json = ttk.Frame(tabbed_config_frame)
        self.build_json(json)
        tabbed_config_frame.add(json, text="JSON")

    def build_common(self, frame):
        """
        Populates the provided frame with all of the fields for the common config.
        :param frame: The frame to populate
        """
        for index, item in enumerate(self.common_items):
            item.create(frame, index, self.data_access)

    def build_use(self, frame):
        """
        Populates the provided frame with all of the fields for the 'use' command.
        :param frame: The frame to populate
        """
        for index, item in enumerate(self.use_items):
            item.create(frame, index, self.data_access)

        b = tk.Button(frame, text="Use", command=partial(self.command, "use", self.use_items + self.common_items))
        b.grid(row=len(self.use_items), column=0, sticky=tk.W)

    def build_create(self, frame):
        """
        Populates the provided frame with all of the fields for the 'create' command.
        :param frame: The frame to populate
        """
        for index, item in enumerate(self.create_items):
            item.create(frame, index, self.data_access)

        b = tk.Button(frame, text="Create", command=partial(self.command, "create", self.create_items + self.common_items))
        b.grid(row=len(self.create_items), column=0, sticky=tk.W)

    def build_json(self, frame):
        """
        Populates the provided frame with all of the fields for the 'build_json' command.
        :param frame: The frame to populate
        """
        for index, item in enumerate(self.json_items):
            item.create(frame, index, self.data_access)

        b = tk.Button(frame, text="Save JSON", command=partial(self.command, "generate_json", self.json_items + self.common_items))
        b.grid(row=len(self.json_items), column=0, sticky=tk.W)


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

    root.mainloop()


if __name__ == "__main__":
    run_gui(NullAPI(), )
