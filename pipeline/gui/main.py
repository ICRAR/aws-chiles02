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
from functools import partial


def pluralise(number, value):
    """
    Pluralise 'value' in the form "value is" or "values are"
    :param number: The number of things we're describing with 'value'
    :param value: The base word to pluralise
    :return: Pluralised value.
    """
    if number == 1 or number == -1:
        return "{0} is".format(value)
    else:
        return "{0}s are".format(value)


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


class ConfigOption:
    """
    Represents a prototype of a single configuration option. These are used to
    build tkinter objects.
    """
    def __init__(self, id, name, t, **kwargs):
        """
        Create a new ConfigOption
        :param id: The unique identifier of the option.
        :param name: The shown label name for the option.
        :param t: The type of the option (input, select, check, file)
        :param kwargs: Keyword arguments for the option. default or data_type are valid.
        """
        self.id = id
        self.name = name
        self.type = t
        self.kwargs = kwargs
        self.default = ''
        self.data_type = str

        if "default" in kwargs:
            self.default = kwargs["default"]
            self.data_type = type(self.default)

        if "data_type" in kwargs:
            self.data_type = kwargs["data_type"]

    @staticmethod
    def file_picker(text):
        """
        Opens a file picker dialogue, and sets the selected value in the provided StringVar
        :param text: A tkinter string var to set the selected path for.
        """
        filename = tkFileDialog.asksaveasfilename(title="Choose a file")
        text.set(filename)

    def validate_and_convert(self, value):
        """
        Tries to convert the given value to the type required by this prototype.
        :param value: The value to convert
        :return: The converted value
        :exception: FieldValidationException if the value cannot be converted to the required type
        """
        try:
            return self.data_type(value)
        except Exception:
            raise FieldValidationException(self.name, value, self.data_type)

    def validate_and_show_error(self, value, label):
        """
        Validates the given value, and sets the background of the given label
        to red if the value is invalid. The label is set back to white if the valid is valid.
        :param value: The value to validate
        :param label: The label to set if the value is invalid.
        :return:
        """
        try:
            value = self.data_type(value)
            label.config(background="white")
        except Exception:
            label.config(background="IndianRed1")

    def create(self, parent, row, data_read):
        """
        Creates a tkinter object from this prototype.
        :param parent: The parent for the object
        :param row: The current row in the parent this object should be placed in
        :param data_read: The dictionary that contains data access functions
        """
        label = tk.Label(parent, text=self.name + ":", justify=tk.LEFT)
        label.grid(row=row, sticky=tk.W)

        if self.type == "input":
            var = tk.StringVar(parent, str(self.default))
            e = tk.Entry(parent, textvariable=var)
            var.trace('w', lambda name, index, mode: self.validate_and_show_error(var.get(), e))
            data_read[self.id] = lambda: self.validate_and_convert(e.get())

        elif self.type == "select":
            var = tk.StringVar(parent, str(self.default))
            e = tk.OptionMenu(parent, var, *self.kwargs["options"])
            data_read[self.id] = lambda: self.validate_and_convert(var.get())

        elif self.type == "check":
            var = tk.IntVar(parent, bool(self.default))
            e = tk.Checkbutton(parent, variable=var)
            data_read[self.id] = lambda: var.get() == 1

        elif self.type == "file":
            e = tk.Frame(parent)
            text = tk.StringVar(parent, str(self.default))
            child = tk.Entry(e, textvariable=text)
            child.pack(side=tk.LEFT, padx=2, pady=2, fill=tk.X, expand=True)
            child = tk.Button(e, text="Browse...", command=partial(self.file_picker, text))
            child.pack(side=tk.LEFT, padx=2, pady=2, fill=tk.X, expand=True)
            data_read[self.id] = lambda: self.validate_and_convert(text.get())

        else:
            return

        e.grid(row=row, column=1)


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

    option_bucket = ConfigOption("bucket", "Bucket Name", "input", default='13b-266')
    option_frequency = ConfigOption("frequency", "Frequency Width", "input", default=4)
    option_iterations = ConfigOption("iterations", "Iterations", "input", default=1)
    option_arc_seconds = ConfigOption("arc_seconds", "Arc Seconds", "input", default=2)
    option_w_projection_planes = ConfigOption("w_projection_planes", "W Projection Planes", "input", default=24)
    option_clean_weighting_uv = ConfigOption("clean_weighting_uv", "Clean Weighting UV", "select", options=weighting_uv_options, default='Briggs')
    option_clean_robust = ConfigOption("clean_robust", "Clean Robust Value", "input", default=0.8)
    option_image_size = ConfigOption("image_size", "Image Size", "input", default=4096)
    option_clean_channel_average = ConfigOption("clean_channel_average", "Input channels to average", "input", default=1)
    option_region_file = ConfigOption("region_file", "Region file", "input")
    option_only_image = ConfigOption("only_image", "Only copy image to S3", "check")
    option_shutdown = ConfigOption("shutdown", "Add shutdown node", "check", default=True)
    option_build_fits = ConfigOption("build_fits", "Build FITS for JPEG2000", "check")
    option_fits_directory_name = ConfigOption("fits_directory_name", "FITS directory", "input")
    option_uvsub_directory_name = ConfigOption("uvsub_directory_name", "UVSUB directory", "input")
    option_clean_directory_name = ConfigOption("clean_directory_name", "Clean directory", "input")
    option_produce_qa = ConfigOption("produce_qa", "Produce QA products", "check")
    option_clean_tclean = ConfigOption("clean_tclean", "Clean or TClean", "select", options=clean_tclean_options, default="clean")
    option_use_bash = ConfigOption("use_bash", "Run CASA in Bash", "check")
    option_casa_version = ConfigOption("casa_version", "CASA version", "select", options=casa_version_options, default='5.1')
    option_volume = ConfigOption("volume", "Host directory for Docker", "input")
    option_frequency_range = ConfigOption("frequency_range", "Frequeny Ranges", "input")
    option_run_note_clean = ConfigOption("run_note_clean", "Note", "input", default='No note')
    option_ami = ConfigOption("ami", "AMI ID", "input")
    option_spot_price_i3_4xlarge = ConfigOption("spot_price_i3_4xlarge", "Spot Price for i3.4xlarge", "input")
    option_frequencies_per_node = ConfigOption("frequencies_per_node", "Frequencies per node", "input", default=1)
    option_log_level = ConfigOption("log_level", "Log level", "select", options=log_level_options, default='vvv')
    option_dim = ConfigOption("dim", "Data island manager IP", "input")
    option_nodes = ConfigOption("nodes", "Node count", "input", default=1)
    option_json = ConfigOption("json", "JSON output path", "file", default='/tmp/json_clean.txt')

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

    def __init__(self, root, api):
        self.root = root
        self.api = api

        self.toolbar = None
        self.main = None

        self.error_label = tk.StringVar()

        self.data_read = {}

        self.build_toolbar()
        self.build_main()

    def get_data_state(self, items):
        out = {}
        errors = []

        for item in items:
            ident = item.id

            try:
                out[ident] = self.data_read[ident]()
            except FieldValidationException as e:
                errors.append(e)

        if len(errors):
            raise ValidationException(*errors)

        return out

    def command(self, what, data_items):
        try:
            data = self.get_data_state(data_items)

            self.error_label.set("")

            f = getattr(self.api, what, None)

            if f is not None:
                f(data)

        except ValidationException as e:
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

    def build_toolbar(self):
        self.toolbar = tk.Frame(self.root, borderwidth=1, relief=tk.GROOVE)

        label = tk.Label(self.toolbar, textvariable=self.error_label)
        label.pack(side=tk.BOTTOM, fill=tk.X, expand=True)

        self.toolbar.pack(side=tk.BOTTOM, fill=tk.X)

    def build_main(self):
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
        for index, item in enumerate(self.common_items):
            item.create(frame, index, self.data_read)

    def build_use(self, frame):
        for index, item in enumerate(self.use_items):
            item.create(frame, index, self.data_read)

        b = tk.Button(frame, text="Use", command=partial(self.command, "use", self.use_items + self.common_items))
        b.grid(row=len(self.use_items), column=0, sticky=tk.W)

    def build_create(self, frame):
        for index, item in enumerate(self.create_items):
            item.create(frame, index, self.data_read)

        b = tk.Button(frame, text="Create", command=partial(self.command, "create", self.create_items + self.common_items))
        b.grid(row=len(self.create_items), column=0, sticky=tk.W)

    def build_json(self, frame):
        for index, item in enumerate(self.json_items):
            item.create(frame, index, self.data_read)

        b = tk.Button(frame, text="Save JSON", command=partial(self.command, "generate_json", self.json_items + self.common_items))
        b.grid(row=len(self.json_items), column=0, sticky=tk.W)


def run_gui(api):
    """
    Runs the GUI for chiles
    :param api: The API for the rest of the program.
    :return:
    """
    root = tk.Tk()
    root.resizable(0, 0)
    gui = ChilesGUI(root, api)

    root.mainloop()


if __name__ == "__main__":
    run_gui(NullAPI())
