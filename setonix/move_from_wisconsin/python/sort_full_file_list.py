#
#  ICRAR - International Centre for Radio Astronomy Research
#  UWA - The University of Western Australia
#
#  Copyright (c) 2022.
#  Copyright by UWA (in the framework of the ICRAR)
#  All rights reserved
#
#  This library is free software; you can redistribute it and/or
#  modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.
#
#  This library is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public
#  License along with this library; if not, write to the Free Software
#  Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#  MA 02111-1307  USA
#
from os.path import dirname
from typing import List

from humanfriendly import format_size
from typer import Typer

FULL_FILE_FILES = "../scripts/file_copy/full_file_list.txt"

app = Typer()


def read_lines():
    with open(FULL_FILE_FILES) as f:
        lines = [line.rstrip() for line in f]

    return lines


def process_file():
    lines = read_lines()
    file_list = []

    current_directory = None
    for line in lines:
        elements = line.split()
        if len(elements) == 1 and elements[0].startswith("/mnt/cephfs"):
            # line: /mnt/cephfs/...
            print(f"cd {elements[0]}")
            current_directory = elements[0][:-1]

        elif (len(elements) == 2 and elements[0].startswith("total")) or len(
            elements
        ) == 0:
            # line: total 1234
            pass

        elif len(elements) == 9:
            # line: -rw-r--r-- 1 root root 1234 Jan 1 12:34 file_name
            if elements[0].startswith("-") and len(elements[0]) == 10:
                file_list.append(
                    (f"{current_directory}/{elements[-1]}", int(elements[4]))
                )

        else:
            raise RuntimeError(f"Unknown line: {line}")

    return file_list


def get_file_details(boundaries: List[float]):
    files = process_file()
    groupings = {}

    for file in files:
        size = file[1]
        added = False
        for boundary in boundaries:
            if size < boundary:
                label = f"lt_{format_size(boundary)}"
                if label not in groupings:
                    groupings[label] = []

                groupings[label].append(file[0])
                added = True
                break

        if not added:
            label = f"gt_{format_size(boundaries[-1])}"
            if label not in groupings:
                groupings[label] = []

            groupings[label].append(file[0])

    for grouping_ in groupings.values():
        grouping_.sort(key=lambda x: x[1], reverse=True)

    return groupings


def get_semester(string: str):
    if string.startswith("2013") or string.startswith("2014"):
        return "2013-2014"
    if string.startswith("2015"):
        return "2015"
    if string.startswith("2016"):
        return "2016"
    if string.startswith("2017") or string.startswith("2018"):
        return "2017-2018"
    if string.startswith("2019"):
        return "2019"

    raise RuntimeError(f"Unknown semester: {string}")


def chiles_name(name: str, bucket: str = "chiles"):
    elements = name.split("/")
    if (
        elements[0] != ""
        or elements[1] != "mnt"
        or elements[2] != "cephfs"
        or elements[3] != "projects"
        or elements[4] != "wilcots"
    ):
        raise RuntimeError(f"Unknown name: {name}")

    tail = "/".join(elements[7:])
    semester = get_semester(elements[5])
    return f"{bucket}/originals/{semester}/{elements[5]}/{tail}"


def safe_name(name: str):
    return (
        name.replace(" ", "_").replace("<", "lt").replace(">", "gt").replace(":", "_")
    )


@app.command()
def scratch_touch(filename: str, boundaries: List[float]):
    groupings = get_file_details(boundaries)

    for grouping, list_ in groupings.items():
        print(f"Grouping: {grouping}")
        name_copy = safe_name(f"{filename}_{grouping}.sh")
        with open(name_copy, "w") as file_:
            file_.write("#!/bin/bash\n")
            for row in list_:
                chiles_name_ = chiles_name(row)
                file_.write(
                    f"mkdir -p /scratch/pawsey0411/kvinsen/{dirname(chiles_name_)}\n"
                )
                file_.write(f"touch /scratch/pawsey0411/kvinsen/{chiles_name_} \n")


@app.command()
def pawsey_copy(filename: str, boundaries: List[float]):
    groupings = get_file_details(boundaries)

    name_copy = None
    name_mkdir = None
    for grouping, list_ in groupings.items():
        print(f"Grouping: {grouping}")
        mkdir_commands = set()
        name_copy = safe_name(f"{filename}_{grouping}.txt")
        with open(name_copy, "w") as file_:
            for row in list_:
                chiles_name_ = chiles_name(row)
                mkdir_commands.add(
                    f"-i ~/.ssh/pawsey kvinsen@data-mover.pawsey.org.au mkdir -p "
                    f"/scratch/pawsey0411/kvinsen/{chiles_name_[:chiles_name_.rfind('/')]}"
                )

                # The space at the end of the line is important for xargs
                file_.write(
                    f"--progress "
                    f"--size-only "
                    f"--inplace "
                    f"-rlvh "
                    f"-e "
                    f'"ssh -i ~/.ssh/pawsey" '
                    f"{row} "
                    f"kvinsen@data-mover.pawsey.org.au:/scratch/pawsey0411/kvinsen/{chiles_name_} \n"
                )

        name_mkdir = safe_name(f"{filename}_mkdir_{grouping}.txt")
        with open(name_mkdir, "w") as file_:
            for row in sorted(mkdir_commands):
                # The space at the end of the line is important for xargs
                file_.write(f"{row} \n")

    print("Inside a screen session")
    print(f"xargs -a {name_mkdir} -P 4 -n 6 -t ssh > {name_mkdir}.log")
    print(f"xargs -a {name_copy} -P 4 -n 8 -t rsync > {name_copy}.log 2>&1")


@app.command()
def acacia_copy(filename: str, boundaries: List[float]):
    groupings = get_file_details(boundaries)

    name_copy = None
    for grouping, list_ in groupings.items():
        print(f"Grouping: {grouping}")
        name_copy = safe_name(f"{filename}_{grouping}.txt")
        with open(name_copy, "w") as file_:
            for row in list_:
                # The space at the end of the line is important for xargs
                file_.write(
                    f"copyto {row} acacia-pawsey0411:{chiles_name(row)} --progress --size-only \n"
                )

    print(
        f"nohup xargs -a {name_copy} -P 4 -n 5 -t ~/rclone/rclone > {name_copy}.log 2>&1 &"
    )


@app.command()
def scratch_to_acacia(filename: str, boundaries: List[float]):
    groupings = get_file_details(boundaries)

    name_copy = None
    for grouping, list_ in groupings.items():
        print(f"Grouping: {grouping}")
        name_copy = safe_name(f"{filename}_{grouping}.txt")
        with open(name_copy, "w") as file_:
            for row in list_:
                chiles_name_ = chiles_name(row)
                chiles_name_01 = chiles_name(row, "chiles01")
                file_.write(
                    f"rclone sync /scratch/pawsey0411/kvinsen/{chiles_name_} "
                    f"acacia-dingo:{chiles_name_01} "
                    f"--progress --size-only --s3-disable-checksum --s3-upload-concurrency 8 --s3-chunk-size 128M\n"
                )

    print(f"nohup {name_copy} > {name_copy}.log 2>&1 &")


if __name__ == "__main__":
    app()
