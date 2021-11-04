#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved 2021
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
Calculate the size of a bucket
"""
import argparse
import logging
from collections import defaultdict

import boto3

LOG = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser("Delete hidden DeleteMarkers in a bucket")
    parser.add_argument("bucket", help="the bucket to access")
    parser.add_argument(
        "--dry-run",
        help="perform a dry run with deleting anything",
        action="store_true",
    )
    return parser.parse_args()


def main():
    arguments = parse_arguments()
    logging.basicConfig(level=logging.INFO)

    session = boto3.Session(profile_name="aws-chiles02")
    s3 = session.client("s3")
    paginator = s3.get_paginator("list_object_versions")

    # Load the Versions and DeleteMarkers
    delete_markers = []
    page_iterator = paginator.paginate(Bucket=arguments.bucket)
    for count, page in enumerate(page_iterator, start=1):
        if "DeleteMarkers" in page:
            delete_markers.extend(page["DeleteMarkers"])

        LOG.info(f"Page: {count}, DeleteMarkers: {len(delete_markers)}")

    # Holds all version IDs for objects w/delete markers and aren't latest
    del_obj_list = defaultdict(list)

    # All delete markers
    for item in delete_markers:
        del_obj_list[item["Key"]].append(item["VersionId"])

    # Remove old delete markers of object by VersionId
    s3_resource = session.resource("s3", use_ssl=False)
    bucket = s3_resource.Bucket(arguments.bucket)
    total = len(del_obj_list)
    for count, (del_item, value) in enumerate(del_obj_list.items(), start=1):
        LOG.info(f"Deleting {del_item} - {count} of {total}")
        rm_obj = bucket.Object(del_item)
        # Remove previous versions
        for del_id in value:
            LOG.info(f"Deleting {del_item} - {del_id}")
            if not arguments.dry_run:
                rm_obj.delete(VersionId=del_id)


if __name__ == "__main__":
    main()
