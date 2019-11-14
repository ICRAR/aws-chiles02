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
from http import HTTPStatus
from http.client import HTTPConnection
from os.path import exists

import boto3
from dlg.droputils import get_roots
from dlg.manager.client import DataIslandManagerClient

from aws_chiles02.build_graph_uvsub import BuildGraphUvsub
from aws_chiles02.common import (
    FrequencyPair,
    get_aws_credentials,
    get_required_frequencies,
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
    WAIT_TIMEOUT_ISLAND_MANAGER,
    WAIT_TIMEOUT_NODE_MANAGER,
)
from aws_chiles02.user_data import (
    get_data_island_manager_user_data,
    get_node_manager_user_data,
)

LOGGER = logging.getLogger(__name__)
PARALLEL_STREAMS = 6


class WorkToDo:
    def __init__(self, **keywords):
        self._width = keywords["width"]
        self._bucket_name = keywords["bucket_name"]
        self._s3_uvsub_name = keywords["s3_uvsub_name"]
        self._s3_split_name = keywords["s3_split_name"]
        self._frequency_range = get_required_frequencies(
            keywords["frequency_range"], self._width
        )

        self._work_already_done = None
        self._bucket = None
        self._work_to_do = []

    def calculate_work_to_do(self):
        session = boto3.Session(profile_name="aws-chiles02")
        s3 = session.resource("s3", use_ssl=False)

        uvsub_objects = []
        self._bucket = s3.Bucket(self._bucket_name)
        for key in self._bucket.objects.filter(
            Prefix="{0}".format(self._s3_uvsub_name)
        ):
            uvsub_objects.append(key.key)
            LOGGER.info("uvsub {0} found".format(key.key))

        for key in self._bucket.objects.filter(
            Prefix="{0}".format(self._s3_split_name)
        ):
            LOGGER.info("split {0} found".format(key.key))
            elements = key.key.split("/")

            if len(elements) >= 2:
                if elements[1].startswith("logs") or elements[1].startswith(
                    "aa_parameter_data.json"
                ):
                    continue

            if len(elements) == 3:
                expected_uvsub_name = "{0}/{1}/{2}".format(
                    self._s3_uvsub_name, elements[1], elements[2]
                )
                if expected_uvsub_name not in uvsub_objects:
                    # Use the frequency
                    frequencies = elements[1].split("_")
                    frequency_pair = FrequencyPair(frequencies[0], frequencies[1])
                    if (
                        self._frequency_range is not None
                        and frequency_pair not in self._frequency_range
                    ):
                        continue

                    self._work_to_do.append([elements[1], elements[2]])

    @property
    def work_to_do(self):
        return self._work_to_do


def get_nodes_required(node_count, spot_price):
    nodes = [
        {
            "number_instances": node_count,
            "instance_type": "i3.4xlarge",
            "spot_price": spot_price,
        }
    ]

    return nodes, node_count


def create_and_generate(**keywords):
    boto_data = get_aws_credentials("aws-chiles02")
    if boto_data is not None:
        bucket_name = keywords["bucket_name"]
        work_to_do = WorkToDo(
            width=keywords["frequency_width"],
            bucket_name=bucket_name,
            s3_uvsub_name=keywords["uvsub_directory_name"],
            s3_split_name=keywords["split_directory"],
            frequency_range=keywords["frequency_range"],
        )
        work_to_do.calculate_work_to_do()

        nodes = keywords["nodes"]
        spot_price = keywords["spot_price"]
        ami_id = keywords["ami_id"]

        nodes_required, node_count = get_nodes_required(nodes, spot_price)

        if len(nodes_required) > 0:
            uuid = get_uuid()
            ec2_data = EC2Controller(
                ami_id,
                nodes_required,
                get_node_manager_user_data(
                    boto_data,
                    uuid,
                    max_request_size=50,
                    chiles=not keywords["use_bash"],
                    casa_version=keywords["casa_version"],
                ),
                AWS_REGION,
                tags=[
                    {"Key": "Owner", "Value": getpass.getuser()},
                    {"Key": "Name", "Value": "DALiuGE NM - Uvsub"},
                    {"Key": "uuid", "Value": uuid},
                ],
            )
            ec2_data.start_instances()

            reported_running = get_reported_running(
                uuid, node_count, wait=WAIT_TIMEOUT_NODE_MANAGER
            )

            if len(reported_running) == 0:
                LOGGER.error("Nothing has reported ready")
            else:
                hosts = build_hosts(reported_running)

                # Create the Data Island Manager
                data_island_manager = EC2Controller(
                    ami_id,
                    [
                        {
                            "number_instances": 1,
                            "instance_type": "m4.large",
                            "spot_price": spot_price,
                        }
                    ],
                    get_data_island_manager_user_data(
                        boto_data, hosts, uuid, max_request_size=50
                    ),
                    AWS_REGION,
                    tags=[
                        {"Key": "Owner", "Value": getpass.getuser()},
                        {"Key": "Name", "Value": "DALiuGE DIM - Uvsub"},
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
                    graph = BuildGraphUvsub(
                        work_to_do=work_to_do.work_to_do,
                        bucket_name=bucket_name,
                        volume=keywords["volume"],
                        parallel_streams=PARALLEL_STREAMS,
                        node_details=reported_running,
                        shutdown=keywords["add_shutdown"],
                        scan_statistics=keywords["scan_statistics"],
                        width=keywords["frequency_width"],
                        w_projection_planes=keywords["w_projection_planes"],
                        number_taylor_terms=keywords["number_taylor_terms"],
                        uvsub_directory_name=keywords["uvsub_directory_name"],
                        session_id=session_id,
                        dim_ip=host,
                        run_note=keywords["run_note"],
                        mode=keywords["mode"],
                        use_bash=keywords["use_bash"],
                        split_directory=keywords["split_directory"],
                        casa_version=keywords["casa_version"],
                        produce_qa=keywords["produce_qa"],
                        s3_storage_class=keywords["s3_storage_class"],
                        s3_tags=convert_yaml_list(keywords["s3_tags"]),
                    )
                    graph.build_graph()

                    if keywords["dump_json"]:
                        json_dumps = json.dumps(graph.drop_list, indent=2)
                        with open(
                            keywords.get("json_path", "/tmp/json_uvsub.txt"), "w"
                        ) as json_file:
                            json_file.write(json_dumps)

                    LOGGER.info("Connection to {0}:{1}".format(host, DIM_PORT))
                    client = DataIslandManagerClient(host, DIM_PORT, timeout=30)

                    client.create_session(session_id)
                    client.append_graph(session_id, graph.drop_list)
                    client.deploy_session(session_id, get_roots(graph.drop_list))
    else:
        LOGGER.error("Unable to find the AWS credentials")


def use_and_generate(**keywords):
    boto_data = get_aws_credentials("aws-chiles02")
    if boto_data is not None:
        host = keywords["host"]
        port = keywords["port"]
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
            frequency_width = keywords["frequency_width"]
            bucket_name = keywords["bucket_name"]
            uvsub_directory_name = keywords["uvsub_directory_name"]
            work_to_do = WorkToDo(
                width=frequency_width,
                bucket_name=bucket_name,
                s3_uvsub_name=uvsub_directory_name,
                s3_split_name=keywords["split_directory"],
                frequency_range=keywords["frequency_range"],
            )
            work_to_do.calculate_work_to_do()

            # Now build the graph
            session_id = get_session_id()
            graph = BuildGraphUvsub(
                work_to_do=work_to_do.work_to_do,
                bucket_name=bucket_name,
                volume=keywords["volume"],
                parallel_streams=PARALLEL_STREAMS,
                node_details=nodes_running,
                shutdown=keywords["add_shutdown"],
                scan_statistics=keywords["scan_statistics"],
                width=frequency_width,
                w_projection_planes=keywords["w_projection_planes"],
                number_taylor_terms=keywords["number_taylor_terms"],
                uvsub_directory_name=uvsub_directory_name,
                session_id=session_id,
                dim_ip=host,
                run_note=keywords["run_note"],
                mode=keywords["mode"],
                use_bash=keywords["use_bash"],
                split_directory=keywords["split_directory"],
                casa_version=keywords["casa_version"],
                produce_qa=keywords["produce_qa"],
                s3_storage_class=keywords["s3_storage_class"],
                s3_tags=convert_yaml_list(keywords["s3_tags"]),
            )
            graph.build_graph()

            if keywords["dump_json"]:
                json_dumps = json.dumps(graph.drop_list, indent=2)
                with open(
                    keywords.get("json_path", "/tmp/json_uvsub.txt"), "w"
                ) as json_file:
                    json_file.write(json_dumps)

            LOGGER.info("Connection to {0}:{1}".format(host, port))
            client = DataIslandManagerClient(host, port, timeout=30)

            client.create_session(session_id)
            client.append_graph(session_id, graph.drop_list)
            client.deploy_session(session_id, get_roots(graph.drop_list))

        else:
            LOGGER.warning("No nodes are running")


def generate_json(**keywords):
    width = keywords["width"]
    bucket = keywords["bucket"]
    uvsub_directory_name = keywords["uvsub_directory_name"]
    work_to_do = WorkToDo(
        width=width,
        bucket_name=bucket,
        s3_uvsub_name=uvsub_directory_name,
        s3_split_name=keywords["split_directory"],
        frequency_range=keywords["frequency_range"],
    )
    work_to_do.calculate_work_to_do()

    node_details = {
        "i3.2xlarge": [
            {"ip_address": "node_i2_{0}".format(i)} for i in range(0, keywords["nodes"])
        ]
    }
    graph = BuildGraphUvsub(
        work_to_do=work_to_do.work_to_do,
        bucket_name=bucket,
        volume=keywords["volume"],
        parallel_streams=PARALLEL_STREAMS,
        node_details=node_details,
        shutdown=keywords["shutdown"],
        scan_statistics=keywords["scan_statistics"],
        width=width,
        w_projection_planes=keywords["w_projection_planes"],
        number_taylor_terms=keywords["number_taylor_terms"],
        uvsub_directory_name=uvsub_directory_name,
        session_id="session_id",
        dim_ip="1.2.3.4",
        run_note=keywords["run_note"],
        mode=keywords["mode"],
        use_bash=keywords["use_bash"],
        split_directory=keywords["split_directory"],
        casa_version=keywords["casa_version"],
        produce_qa=keywords["produce_qa"],
        s3_storage_class=keywords["s3_storage_class"],
        s3_tags=convert_yaml_list(keywords["s3_tags"]),
    )
    graph.build_graph()
    json_dumps = json.dumps(graph.drop_list, indent=2)
    LOGGER.info(json_dumps)
    with open(keywords.get("json_path", "/tmp/json_uvsub.txt"), "w") as json_file:
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
    if config["action"] != "uvsub":
        LOGGER.error(
            "Invalid tag: {} for {}".format(command_line_.tag_name, config["action"])
        )
        return

    # Run the command
    if config["run_type"] == "create":
        create_and_generate(
            bucket_name=config["bucket_name"],
            frequency_width=config["width"],
            w_projection_planes=config["w_projection_planes"],
            number_taylor_terms=config["number_taylor_terms"],
            ami_id=config["ami"],
            spot_price=config["spot_price_i3_4xlarge"],
            volume=config["volume"],
            nodes=config["nodes"],
            add_shutdown=config["shutdown"],
            frequency_range=config["frequency_range"],
            scan_statistics=config["scan_statistics"],
            uvsub_directory_name=config["uvsub_directory_name"],
            dump_json=config["dump_json"],
            run_note=config["run_note_uvsub"],
            mode=config["mode"],
            use_bash=config["use_bash"],
            casa_version=config["casa_version"],
            split_directory=config["split_directory"],
            produce_qa=config["produce_qa"],
            s3_storage_class=config["s3_storage_class"],
            s3_tags=config["s3_tags"] if "s3_tags" in config else None,
        )
    elif config["run_type"] == "use":
        use_and_generate(
            host=config["dim"],
            port=DIM_PORT,
            bucket_name=config["bucket_name"],
            frequency_width=config["width"],
            w_projection_planes=config["w_projection_planes"],
            number_taylor_terms=config["number_taylor_terms"],
            volume=config["volume"],
            add_shutdown=config["shutdown"],
            frequency_range=config["frequency_range"],
            scan_statistics=config["scan_statistics"],
            uvsub_directory_name=config["uvsub_directory_name"],
            dump_json=config["dump_json"],
            run_note=config["run_note_uvsub"],
            mode=config["mode"],
            use_bash=config["use_bash"],
            casa_version=config["casa_version"],
            split_directory=config["split_directory"],
            produce_qa=config["produce_qa"],
            s3_storage_class=config["s3_storage_class"],
            s3_tags=config["s3_tags"] if "s3_tags" in config else None,
        )
    else:
        generate_json(
            width=config["width"],
            w_projection_planes=config["w_projection_planes"],
            number_taylor_terms=config["number_taylor_terms"],
            bucket=config["bucket_name"],
            nodes=config["nodes"],
            volume=config["volume"],
            shutdown=config["shutdown"],
            frequency_range=config["frequency_range"],
            scan_statistics=config["scan_statistics"],
            uvsub_directory_name=config["uvsub_directory_name"],
            run_note=config["run_note_uvsub"],
            mode=config["mode"],
            use_bash=config["use_bash"],
            casa_version=config["casa_version"],
            split_directory=config["split_directory"],
            produce_qa=config["produce_qa"],
            s3_storage_class=config["s3_storage_class"],
            s3_tags=config["s3_tags"] if "s3_tags" in config else None,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UV Sub")
    parser.add_argument(
        "--config_file", default=None, help="the config file for this run"
    )
    parser.add_argument(
        "tag_name", nargs="?", default="uvsub", help="the tag name to execute"
    )
    command_line = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="{asctime}:{levelname}:{name}:{message}", style="{"
    )
    run(command_line)
