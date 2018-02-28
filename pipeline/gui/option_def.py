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
Defines the options to use for specific task, action combinations
"""

from option import Input, Select, Check, ChooseFile
from validation import Int, Float, String, SelectList

# All possible tasks the user can perform
task_options = [
    "Clean Graph",
    "Image Concatenate",
    "JPEG2000 Graph",
    "MSTransform Graph",
    "UVSub Graph"
]

# All actions the user can select for each task
action_options = [
    "Generate JSON",
    "Create Graph",
    "Use Graph"
]

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

observation_phase_option = [
    "1",
    "2"
]

option_bucket_name = Input("bucket_name", "Bucket Name", String(), default='13b-266')
option_frequency_width = Input("frequency", "Frequency Width", Float(), default=4)
option_imageconcat_width = Input("imageconcat_width", "Image Concat Frequency Width", Int(), default=6)
option_iterations = Input("iterations", "Iterations", Int(), default=1)
option_arc_seconds = Input("arc_seconds", "Arc Seconds", Float(), default=2)
option_w_projection_planes = Input("w_projection_planes", "W Projection Planes", Int(), default=24)
option_clean_weighting_uv = Select("clean_weighting_uv", "Clean Weighting UV", weighting_uv_options, default='Briggs')
option_clean_robust = Input("clean_robust", "Clean Robust Value", Float(), default=0.8)
option_image_size = Input("image_size", "Image Size", Int(), default=4096)
option_clean_channel_average = Input("clean_channel_average", "Input channels to average", Int(), default=1)
option_region_file = Input("region_file", "Region file", String())
option_only_image = Check("only_image", "Only copy image to S3")
option_add_shutdown_node = Check("add_shutdown_node", "Add shutdown node", default=True)
option_build_fits = Check("build_fits", "Build FITS for JPEG2000")
option_fits_directory_name = Input("fits_directory_name", "FITS directory", String())
option_uvsub_directory_name = Input("uvsub_directory_name", "UVSUB directory", String())
option_clean_directory_name = Input("clean_directory_name", "Clean directory", String())
option_imageconcat_directory_name = Input("imageconcat_directory_name", "Imageconcat directory", String())
option_jpeg2000_directory_name = Input("jpeg2000_directory_name", "JPEG2000 directory", String())
option_split_directory_name = Input("split_directory_name", "Where to store the split data", String(), default="split_1")
option_produce_qa = Check("produce_qa", "Produce QA products")
option_clean_tclean = Select("clean_tclean", "Clean or TClean", clean_tclean_options, default="clean")
option_use_bash = Check("use_bash", "Run CASA in Bash")
option_casa_version = Select("casa_version", "CASA version", casa_version_options, default='5.1')
option_volume = Input("volume", "Host directory for Docker", String())
option_frequency_range = Input("frequency_range", "Frequeny Ranges", String())
option_run_note = Input("run_note", "Note", String(), default='No note')
option_ami_id = Input("ami_id", "AMI ID", String())
option_spot_price_i3_xlarge = Input("spot_price_i3.xlarge", "Spot Price for i3.xlarge", String())
option_spot_price_i3_4xlarge = Input("spot_price_i3.4xlarge", "Spot Price for i3.4xlarge", String())
option_spot_price_i3_2xlarge = Input("spot_price_i3.2xlarge", "Spot Price for i3.2xlarge", String())
option_frequencies_per_node = Input("frequencies_per_node", "Frequencies per node", Int(), default=1)
option_log_level = Select("log_level", "Log level", log_level_options, default='vvv')
option_dim_ip = Input("dim_ip", "Data island manager IP", String())
option_dim_port = Input("dim_port", "Data island manager port", Int(), default=8001)
option_nodes = Input("nodes", "Node count", Int(), default=1)
option_days_per_node = Input("days_per_node", "Days per node", Int(), default=1)
option_observation_phase = Select("observation_phase", "Observation Phase", observation_phase_option, default="1")
option_parallel_streams = Input("parallel_streams", "Parallel Streams", Int(), default=4)
option_number_taylor_terms = Input("number_taylor_terms", "Number of Taylor Terms", Int(), default=2)
option_scan_statistics = Check("scan_statistics", "Generate scan statistics")
option_absorption = Check("absorption", "Run absorption uvsub")
option_dump_json = Check("dump_json", "Dump the JSON")
option_json_path = ChooseFile("json_path", "JSON output path", default='/tmp/json.txt')

options = {
    "clean": {
        "create": [option_bucket_name,
                   option_frequency_width,
                   option_ami_id,
                   option_spot_price_i3_4xlarge,
                   option_volume,
                   option_frequencies_per_node,
                   option_add_shutdown_node,
                   option_iterations,
                   option_arc_seconds,
                   option_w_projection_planes,
                   option_clean_weighting_uv,
                   option_clean_robust,
                   option_only_image,
                   option_image_size,
                   option_clean_channel_average,
                   option_region_file,
                   option_frequency_range,
                   option_clean_directory_name,
                   option_log_level,
                   option_produce_qa,
                   option_uvsub_directory_name,
                   option_fits_directory_name,
                   option_clean_tclean,
                   option_run_note,
                   option_use_bash,
                   option_casa_version,
                   option_build_fits],

        "use": [option_dim_ip,
                option_dim_port,
                option_bucket_name,
                option_frequency_width,
                option_volume,
                option_add_shutdown_node,
                option_iterations,
                option_arc_seconds,
                option_w_projection_planes,
                option_clean_weighting_uv,
                option_clean_robust,
                option_image_size,
                option_clean_channel_average,
                option_region_file,
                option_frequency_range,
                option_clean_directory_name,
                option_only_image,
                option_produce_qa,
                option_uvsub_directory_name,
                option_fits_directory_name,
                option_clean_tclean,
                option_run_note,
                option_use_bash,
                option_casa_version,
                option_build_fits],

        "json": [option_frequency_width,
                 option_bucket_name,
                 option_iterations,
                 option_arc_seconds,
                 option_nodes,
                 option_volume,
                 option_add_shutdown_node,
                 option_w_projection_planes,
                 option_clean_weighting_uv,
                 option_clean_robust,
                 option_image_size,
                 option_clean_channel_average,
                 option_region_file,
                 option_frequency_range,
                 option_clean_directory_name,
                 option_only_image,
                 option_produce_qa,
                 option_uvsub_directory_name,
                 option_fits_directory_name,
                 option_clean_tclean,
                 option_run_note,
                 option_use_bash,
                 option_casa_version,
                 option_build_fits,
                 option_json_path]
    },

    "imageconcat": {
        "create": [option_frequency_width,
                   option_bucket_name,
                   option_clean_directory_name,
                   option_fits_directory_name,
                   option_imageconcat_directory_name,
                   option_imageconcat_width,
                   option_frequency_range,
                   option_nodes,
                   option_spot_price_i3_xlarge,
                   option_ami_id,
                   option_use_bash,
                   option_casa_version,
                   option_volume,
                   option_add_shutdown_node,
                   option_build_fits,
                   option_run_note],

        "use": [option_dim_ip,
                option_dim_port,
                option_frequency_width,
                option_bucket_name,
                option_clean_directory_name,
                option_fits_directory_name,
                option_imageconcat_directory_name,
                option_imageconcat_width,
                option_frequency_range,
                option_volume,
                option_add_shutdown_node,
                option_use_bash,
                option_casa_version,
                option_build_fits,
                option_run_note],

        "json": [option_frequency_width,
                 option_bucket_name,
                 option_clean_directory_name,
                 option_fits_directory_name,
                 option_imageconcat_directory_name,
                 option_imageconcat_width,
                 option_frequency_range,
                 option_nodes,
                 option_volume,
                 option_add_shutdown_node,
                 option_use_bash,
                 option_casa_version,
                 option_build_fits,
                 option_run_note,
                 option_json_path]
    },

    "jpeg2000": {
        "create": [option_ami_id,
                   option_spot_price_i3_2xlarge,
                   option_bucket_name,
                   option_volume,
                   option_add_shutdown_node,
                   option_fits_directory_name,
                   option_jpeg2000_directory_name],
        "use": [
            option_dim_ip,
            option_dim_port,
            option_bucket_name,
            option_volume,
            option_add_shutdown_node,
            option_fits_directory_name,
            option_jpeg2000_directory_name
        ],

        "json": [
            # Skip?
        ]
    },

    "mstransform": {
        "create": [option_bucket_name,
                   option_frequency_width,
                   option_ami_id,
                   option_spot_price_i3_2xlarge,
                   option_spot_price_i3_4xlarge,
                   option_volume,
                   option_add_shutdown_node,
                   option_use_bash,
                   option_casa_version,
                   option_split_directory_name,
                   option_observation_phase],

        "use": [option_dim_ip,
                option_dim_port,
                option_bucket_name,
                option_frequency_width,
                option_volume,
                option_add_shutdown_node,
                option_use_bash,
                option_casa_version,
                option_split_directory_name,
                option_observation_phase],

        "json": [option_bucket_name,
                 option_frequency_width,
                 option_volume,
                 option_nodes,
                 option_parallel_streams,
                 option_add_shutdown_node,
                 option_use_bash,
                 option_casa_version,
                 option_split_directory_name,
                 option_observation_phase,
                 option_json_path]
    },

    "uvsub": {
        "create": [option_bucket_name,
                   option_frequency_width,
                   option_w_projection_planes,
                   option_number_taylor_terms,
                   option_ami_id,
                   option_spot_price_i3_2xlarge,
                   option_volume,
                   option_nodes,
                   option_add_shutdown_node,
                   option_frequency_range,
                   option_scan_statistics,
                   option_uvsub_directory_name,
                   option_dump_json,
                   option_run_note,
                   option_use_bash,
                   option_casa_version,
                   option_split_directory_name,
                   option_produce_qa,
                   option_absorption],

        "use": [option_dim_ip,
                option_dim_port,
                option_bucket_name,
                option_frequency_width,
                option_w_projection_planes,
                option_number_taylor_terms,
                option_volume,
                option_add_shutdown_node,
                option_frequency_range,
                option_scan_statistics,
                option_uvsub_directory_name,
                option_dump_json,
                option_run_note,
                option_use_bash,
                option_casa_version,
                option_split_directory_name,
                option_produce_qa,
                option_absorption],

        "json": [option_frequency_width,
                 option_w_projection_planes,
                 option_number_taylor_terms,
                 option_bucket_name,
                 option_nodes,
                 option_volume,
                 option_add_shutdown_node,
                 option_frequency_range,
                 option_scan_statistics,
                 option_uvsub_directory_name,
                 option_run_note,
                 option_use_bash,
                 option_casa_version,
                 option_split_directory_name,
                 option_produce_qa,
                 option_absorption,
                 option_json_path]
    }
}
task_map = {
    "Clean Graph": "clean",
    "Image Concatenate": "imageconcat",
    "JPEG2000 Graph": "jpeg2000",
    "MSTransform Graph": "mstransform",
    "UVSub Graph": "uvsub"
}

action_map = {
    "Generate JSON": "json",
    "Create Graph": "create",
    "Use Graph": "use"
}


def get_options(task, action):
    """
    :param task:
    :param action:
    :return:
    """
    if task in task_map:
        task = task_map[task]

    if action in action_map:
        action = action_map[action]

    return options[task][action]
