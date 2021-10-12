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
Build a dictionary for the execution graph
"""
import argparse
import getpass
import json
import logging
import os
import tempfile
from http import HTTPStatus
from http.client import HTTPConnection
from os.path import exists, join

import boto3
from dlg.droputils import get_roots
from dlg.manager.client import DataIslandManagerClient

from aws_chiles02.build_graph_mstransform import BuildGraphMsTransform
from aws_chiles02.common import (
    FrequencyPair,
    MeasurementSetData,
    get_aws_credentials,
    get_list_frequency_groups,
    get_session_id,
    get_uuid,
    get_config,
    convert_yaml_list,
)
from aws_chiles02.ec2_controller import EC2Controller
from aws_chiles02.generate_common import (
    build_hosts,
    get_nodes_running,
    get_reported_running,
)
from aws_chiles02.settings_file import (
    AWS_REGION,
    DIM_PORT,
    SIZE_1GB,
    WAIT_TIMEOUT_ISLAND_MANAGER,
    WAIT_TIMEOUT_NODE_MANAGER,
)
from aws_chiles02.user_data import (
    get_data_island_manager_user_data,
    get_node_manager_user_data,
)

LOGGER = logging.getLogger(__name__)


class WorkToDo(object):
    def __init__(
        self,
        width,
        bucket_name,
        s3_split_name,
        observation_phase,
        source_ms_dir,
        minimum_frequency,
    ):
        self._width = width
        self._bucket_name = bucket_name
        self._s3_split_name = s3_split_name
        self._work_already_done = None
        self._bucket = None
        self._list_frequencies = None
        self._observation_phase = observation_phase
        self._work_to_do = {}
        self._source_ms_dir = source_ms_dir
        self._minimum_frequency = minimum_frequency

    def calculate_work_to_do(self):
        session = boto3.Session(profile_name="aws-chiles02")
        s3 = session.resource("s3", use_ssl=False)

        list_measurement_sets = []
        processed_ms = set()
        self._bucket = s3.Bucket(self._bucket_name)
        for key in self._bucket.objects.filter(Prefix=self._source_ms_dir):
            if key.key.endswith("_calibrated_deepfield.ms.tar") or key.key.endswith(
                "_calibrated_deepfield.ms.tar.gz"
            ):
                LOGGER.info("Found {0}".format(key.key))

                list_measurement_sets.append(
                    MeasurementSetData(key.key, key.size, key.key.endswith("gz"))
                )
            elif "_calibrated_deepfield.ms/" in key.key:
                elements = key.key.split("/")
                ms_key = "/".join(elements[:2])
                if ms_key not in processed_ms:
                    LOGGER.info("Found {0}".format(ms_key))

                    list_objects, total_size = self._get_all_files(ms_key)
                    list_measurement_sets.append(
                        MeasurementSetData(ms_key, total_size, False, list_objects)
                    )
                    processed_ms.add(ms_key)

        # Get work we've already done
        self._list_frequencies = get_list_frequency_groups(
            self._width, minimum_frequency=self._minimum_frequency
        )
        self._work_already_done = self._get_work_already_done()

        for day_to_process in list_measurement_sets:
            day_work_already_done = self._work_already_done.get(
                day_to_process.short_name
            )
            list_frequency_groups = self._get_details_for_measurement_set(
                day_work_already_done
            )

            if self._ignore_day(list_frequency_groups):
                LOGGER.info(
                    "{0} has already been process.".format(
                        day_to_process.input_s3_key_name
                    )
                )
            else:
                self._work_to_do[day_to_process] = list_frequency_groups

    def _get_work_already_done(self):
        frequencies_per_day = {}
        for key in self._bucket.objects.filter(Prefix=self._s3_split_name):
            elements = key.key.split("/")

            if len(elements) > 2:
                day_key = elements[2]

                if day_key.startswith("13B-266."):
                    # Remove the .tar
                    day_key = day_key[:-4]

                    frequencies = frequencies_per_day.get(day_key)
                    if frequencies is None:
                        frequencies = []
                        frequencies_per_day[day_key] = frequencies

                    frequencies.append(elements[1])

        return frequencies_per_day

    def _get_details_for_measurement_set(self, splits_done):
        frequency_groups = []
        if splits_done is None:
            frequency_groups.extend(self._list_frequencies)
        else:
            # Remove the groups we've processed
            for frequency_group in self._list_frequencies:
                if frequency_group.underscore_name not in splits_done:
                    frequency_groups.append(frequency_group)

        return frequency_groups

    def _ignore_day(self, list_frequency_groups):
        # Check if we have the first groups
        count_bottom = 0
        for bottom_frequency in range(940, 956, self._width):
            frequency_group = FrequencyPair(
                bottom_frequency, bottom_frequency + self._width
            )
            if frequency_group in list_frequency_groups:
                count_bottom += 1

        return len(list_frequency_groups) - count_bottom <= 0

    @property
    def work_to_do(self):
        return self._work_to_do

    def _get_all_files(self, ms_key):
        list_files = []
        total_size = 0

        for key in self._bucket.objects.filter(Prefix=ms_key):
            if key.key.endswith("/") and key.size == 0:
                # Ignore this one
                pass
            else:
                LOGGER.info("Found {0}".format(key.key))

                list_files.append(key.key)
                total_size += key.size

        return list_files, total_size


def get_nodes_required(days, days_per_node, spot_price_2xlarge, spot_price_4xlarge):
    nodes = []
    counts = [0, 0, 0]
    for day in days:
        if day.gzipped:
            if day.size <= 25 * SIZE_1GB:
                counts[0] += 1
            elif day.size <= 250 * SIZE_1GB:
                counts[1] += 1
            else:
                counts[2] += 1
        else:
            if day.size <= 50 * SIZE_1GB:
                counts[0] += 1
            elif day.size <= 500 * SIZE_1GB:
                counts[1] += 1
            else:
                counts[2] += 1

    node_count = 0
    if counts[2] > 0:
        count = max(counts[2] // days_per_node, 1)
        node_count += count
        nodes.append(
            {
                "number_instances": count,
                "instance_type": "i3.4xlarge",
                "spot_price": spot_price_4xlarge,
            }
        )
    if counts[1] > 0:
        count = max(counts[1] // days_per_node, 1)
        node_count += count
        nodes.append(
            {
                "number_instances": count,
                "instance_type": "i3.2xlarge",
                "spot_price": spot_price_2xlarge,
            }
        )
    if counts[0] > 0:
        count = max(counts[0] // days_per_node, 1)
        node_count += count
        nodes.append(
            {
                "number_instances": count,
                "instance_type": "i3.xlarge",
                "spot_price": spot_price_2xlarge,
            }
        )

    return nodes, node_count


def create_and_generate(
    bucket_name,
    frequency_width,
    ami_id,
    spot_price1,
    spot_price2,
    volume,
    days_per_node,
    add_shutdown,
    use_bash,
    casa_version,
    split_directory,
    observation_phase,
    run_note,
    s3_storage_class,
    s3_tags,
    source_ms_dir,
    minimum_frequency,
):
    boto_data = get_aws_credentials("aws-chiles02")
    if boto_data is not None:
        work_to_do = WorkToDo(
            width=frequency_width,
            bucket_name=bucket_name,
            s3_split_name=split_directory,
            observation_phase=observation_phase,
            source_ms_dir=source_ms_dir,
            minimum_frequency=minimum_frequency,
        )
        work_to_do.calculate_work_to_do()

        days = work_to_do.work_to_do.keys()
        nodes_required, node_count = get_nodes_required(
            days=days,
            days_per_node=days_per_node,
            spot_price_2xlarge=spot_price1,
            spot_price_4xlarge=spot_price2,
        )

        if len(nodes_required) > 0:
            uuid = get_uuid()
            ec2_data = EC2Controller(
                ami_id,
                nodes_required,
                get_node_manager_user_data(
                    boto_data, uuid, chiles=not use_bash, casa_version=casa_version
                ),
                AWS_REGION,
                tags=[
                    {"Key": "Owner", "Value": getpass.getuser()},
                    {"Key": "Name", "Value": "DALiuGE NM - MsTransform"},
                    {"Key": "uuid", "Value": uuid},
                ],
            )
            ec2_data.start_instances()

            reported_running = get_reported_running(
                uuid, node_count, wait=WAIT_TIMEOUT_NODE_MANAGER
            )
            hosts = build_hosts(reported_running)

            # Create the Data Island Manager
            data_island_manager = EC2Controller(
                ami_id,
                [
                    {
                        "number_instances": 1,
                        "instance_type": "m4.large",
                        "spot_price": spot_price1,
                    }
                ],
                get_data_island_manager_user_data(boto_data, hosts, uuid),
                AWS_REGION,
                tags=[
                    {"Key": "Owner", "Value": getpass.getuser()},
                    {"Key": "Name", "Value": "DALiuGE DIM - MsTransform"},
                    {"Key": "uuid", "Value": uuid},
                ],
            )
            data_island_manager.start_instances()
            data_island_manager_running = get_reported_running(
                uuid, 1, wait=WAIT_TIMEOUT_ISLAND_MANAGER
            )

            if len(data_island_manager_running["m4.large"]) == 1:
                # Now build the graph
                session_id = get_session_id()
                instance_details = data_island_manager_running["m4.large"][0]
                host = instance_details["ip_address"]
                graph = BuildGraphMsTransform(
                    work_to_do=work_to_do.work_to_do,
                    bucket_name=bucket_name,
                    volume=volume,
                    parallel_streams=1,
                    node_details=reported_running,
                    shutdown=add_shutdown,
                    width=frequency_width,
                    session_id=session_id,
                    dim_ip=host,
                    use_bash=use_bash,
                    split_directory=split_directory,
                    observation_phase=observation_phase,
                    casa_version=casa_version,
                    run_note=run_note,
                    s3_storage_class=s3_storage_class,
                    s3_tags=convert_yaml_list(s3_tags),
                )
                graph.build_graph()
                graph.tag_all_app_drops({"session_id": session_id})

                LOGGER.info("Connection to {0}:{1}".format(host, DIM_PORT))
                client = DataIslandManagerClient(host, DIM_PORT, timeout=30)

                client.create_session(session_id)
                client.append_graph(session_id, graph.drop_list)
                client.deploy_session(session_id, get_roots(graph.drop_list))
    else:
        LOGGER.error("Unable to find the AWS credentials")


def use_and_generate(
    host,
    port,
    bucket_name,
    frequency_width,
    volume,
    add_shutdown,
    use_bash,
    split_directory,
    observation_phase,
    casa_version,
    run_note,
    s3_storage_class,
    s3_tags,
    source_ms_dir,
    minimum_frequency,
):
    boto_data = get_aws_credentials("aws-chiles02")
    if boto_data is not None:
        connection = HTTPConnection(host, port)
        connection.request("GET", "/api", None, {})
        response = connection.getresponse()
        if response.status != HTTPStatus.OK:
            msg = "Error while processing GET request for {0}:{1}/api (status {2}): {3}".format(
                host, port, response.status, response.read()
            )
            raise Exception(msg)

        json_data = response.read()
        message_details = json.loads(json_data)
        host_list = message_details["hosts"]

        nodes_running = get_nodes_running(host_list)
        if len(nodes_running) > 0:
            work_to_do = WorkToDo(
                width=frequency_width,
                bucket_name=bucket_name,
                s3_split_name=split_directory,
                observation_phase=observation_phase,
                source_ms_dir=source_ms_dir,
                minimum_frequency=minimum_frequency,
            )
            work_to_do.calculate_work_to_do()

            # Now build the graph
            session_id = get_session_id()
            graph = BuildGraphMsTransform(
                work_to_do=work_to_do.work_to_do,
                bucket_name=bucket_name,
                volume=volume,
                parallel_streams=7,
                node_details=nodes_running,
                shutdown=add_shutdown,
                width=frequency_width,
                session_id=session_id,
                dim_ip=host,
                use_bash=use_bash,
                split_directory=split_directory,
                observation_phase=observation_phase,
                casa_version=casa_version,
                run_note=run_note,
                s3_storage_class=s3_storage_class,
                s3_tags=convert_yaml_list(s3_tags),
            )
            graph.build_graph()

            LOGGER.info("Connection to {0}:{1}".format(host, port))
            client = DataIslandManagerClient(host, port, timeout=30)

            client.create_session(session_id)
            client.append_graph(session_id, graph.drop_list)
            client.deploy_session(session_id, get_roots(graph.drop_list))

        else:
            LOGGER.warning("No nodes are running")


def build_json(
    bucket,
    width,
    volume,
    nodes,
    parallel_streams,
    add_shutdown,
    use_bash,
    split_directory,
    observation_phase,
    casa_version,
    run_note,
    json_path,
    s3_storage_class,
    s3_tags,
    source_ms_dir,
    minimum_frequency,
):
    work_to_do = WorkToDo(
        width,
        bucket,
        split_directory,
        observation_phase,
        source_ms_dir=source_ms_dir,
        minimum_frequency=minimum_frequency,
    )
    work_to_do.calculate_work_to_do()

    node_details = {
        "i3.2xlarge": [{"ip_address": "node_i2_{0}".format(i)} for i in range(nodes)],
        "i3.4xlarge": [{"ip_address": "node_i4_{0}".format(i)} for i in range(nodes)],
    }

    graph = BuildGraphMsTransform(
        work_to_do=work_to_do.work_to_do,
        bucket_name=bucket,
        volume=volume,
        parallel_streams=parallel_streams,
        node_details=node_details,
        shutdown=add_shutdown,
        width=width,
        session_id="json_test",
        dim_ip="1.2.3.4",
        use_bash=use_bash,
        split_directory=split_directory,
        observation_phase=observation_phase,
        casa_version=casa_version,
        run_note=run_note,
        s3_storage_class=s3_storage_class,
        s3_tags=convert_yaml_list(s3_tags),
    )
    graph.build_graph()
    json_dumps = json.dumps(graph.drop_list, indent=2)
    LOGGER.info(json_dumps)
    with open(json_path, "w") as json_file:
        json_file.write(json_dumps)


def run(command_line_):
    if command_line_.config_file is not None:
        if exists(command_line_.config_file):
            yaml_filename = command_line_
        else:
            LOGGER.error(
                "Invalid configuration filename: {}".format(command_line_.config_file)
            )
            return
    else:
        path_dirname, filename = os.path.split(__file__)
        yaml_filename = "{0}/aws-chiles02.yaml".format(path_dirname)

    LOGGER.info("Reading YAML file {}".format(yaml_filename))
    config = get_config(yaml_filename, command_line_.tag_name)
    if config["action"] != "split":
        LOGGER.error(
            "Invalid tag: {} for {}".format(command_line_.tag_name, config["action"])
        )
        return

    # Run the command
    if config["run_type"] == "create":
        create_and_generate(
            bucket_name=config["bucket_name"],
            frequency_width=config["width"],
            ami_id=config["ami"],
            spot_price1=config["spot_price_i3_2xlarge"],
            spot_price2=config["spot_price_i3_4xlarge"],
            volume=config["volume"],
            days_per_node=config["days_per_node"],
            add_shutdown=config["shutdown"],
            use_bash=config["use_bash"],
            casa_version=config["casa_version"],
            split_directory=config["split_directory"],
            observation_phase=config["observation_phase"],
            run_note=config["run_note"],
            s3_storage_class=config["s3_storage_class"],
            s3_tags=config["s3_tags"] if "s3_tags" in config else None,
            source_ms_dir=config["source_ms_dir"]
            if "source_ms_dir" in config
            else None,
            minimum_frequency=config["minimum_frequency"]
            if "minimum_frequency" in config
            else 944,
        )
    elif config["run_type"] == "use":
        use_and_generate(
            host=config["dim"],
            port=DIM_PORT,
            bucket_name=config["bucket_name"],
            frequency_width=config["width"],
            volume=config["volume"],
            add_shutdown=config["shutdown"],
            use_bash=config["use_bash"],
            casa_version=config["casa_version"],
            split_directory=config["split_directory"],
            observation_phase=config["observation_phase"],
            run_note=config["run_note"],
            s3_storage_class=config["s3_storage_class"],
            s3_tags=config["s3_tags"] if "s3_tags" in config else None,
            source_ms_dir=config["source_ms_dir"]
            if "source_ms_dir" in config
            else None,
            minimum_frequency=config["minimum_frequency"]
            if "minimum_frequency" in config
            else 944,
        )
    else:
        build_json(
            bucket=config["bucket_name"],
            width=config["width"],
            volume=config["volume"],
            nodes=config["nodes"],
            parallel_streams=config["parallel_streams"],
            add_shutdown=config["shutdown"],
            use_bash=config["use_bash"],
            casa_version=config["casa_version"],
            split_directory=config["split_directory"],
            observation_phase=config["observation_phase"],
            run_note=config["run_note"],
            json_path=join(tempfile.gettempdir(), "json_file.json"),
            s3_storage_class=config["s3_storage_class"],
            s3_tags=config["s3_tags"] if "s3_tags" in config else None,
            source_ms_dir=config["source_ms_dir"]
            if "source_ms_dir" in config
            else None,
            minimum_frequency=config["minimum_frequency"]
            if "minimum_frequency" in config
            else 944,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split")
    parser.add_argument(
        "--config_file", default=None, help="the config file for this run"
    )
    parser.add_argument(
        "tag_name", nargs="?", default="split", help="the tag name to execute"
    )
    command_line = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="{asctime}:{levelname}:{name}:{message}", style="{"
    )
    run(command_line)
