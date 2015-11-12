#
#    (c) UWA, The University of Western Australia
#    M468/35 Stirling Hwy
#    Perth WA 6009
#    Australia
#
#    Copyright by UWA, 2012-2015
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

"""
import fnmatch
import logging
import argparse
from os import makedirs, walk
from os.path import exists, isdir, join, split

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s:' + logging.BASIC_FORMAT)


def write_script(full_script_filename, full_measurement_set):
    LOG.info('Writing script at: {0}, {1}'.format(full_script_filename, full_measurement_set))

    (script_directory, script_filename) = split(full_script_filename)
    (measurement_set_directory, measurement_set_filename) = split(full_measurement_set)
    output_file = open(full_script_filename, 'w')
    output_file.write('''#!/bin/bash
# Tar up the measurement set - find the MD5 and copy it to S3
cd {0}
tar -cvf {1}/{2}.tar {2}

# Now make the MD5 file
~/jre1.8.0_65/bin/java -cp ~/aws-chiles02/java/build/awsChiles02.jar org.icrar.awsChiles02.copyToS3.GetMD5 {1}/{2}.tar

# Copy the file to S3
~/jre1.8.0_65/bin/java -cp ~/aws-chiles02/java/build/awsChiles02.jar org.icrar.awsChiles02.copyToS3.CopyFileToS3 -aws_profile aws-chiles02 13b-266 {2}.tar {1}/{2}.tar


'''.format(
            measurement_set_directory,
            script_directory,
            measurement_set_filename
        )
        )
    output_file.close()


def write_scripts(list_measurement_sets, root_directory):
    index = 0
    for measurement_set in list_measurement_sets:
        script_dir = join(root_directory, '{0:03d}'.format(index))
        if not exists(script_dir):
            makedirs(script_dir)
        script_filename = join(script_dir, 'copy_measurement_set.sh')
        write_script(script_filename, measurement_set)
        index += 1


def get_list_measurement_sets(directory_in):
    list_measurement_sets = []
    for root, dir_names, filenames in walk(directory_in):
        for match in fnmatch.filter(dir_names, '*_calibrated_deepfield.ms'):
            measurement_set = join(root, match)
            LOG.info('Looking at: {0}'.format(measurement_set))
            list_measurement_sets.append(measurement_set)

    return list_measurement_sets


def build_scripts(args):
    if not exists(args.directory_in) or not isdir(args.directory_in):
        LOG.error('The directory {0} does not exist'.format(args.directory_in))

    if exists(args.directory_out) and not isdir(args.directory_out):
        LOG.error('The directory {0} cannot be created as a file of that name exists'.format(args.directory_out))

    if not exists(args.directory_out):
        makedirs(args.directory_out)

    list_measurement_sets = get_list_measurement_sets(args.directory_in)
    write_scripts(list_measurement_sets, args.directory_out)


def main():
    parser = argparse.ArgumentParser('Get the MD5 of a file')
    parser.add_argument('directory_in', help='the input directory to scan')
    parser.add_argument('directory_out', help='where to write the files')
    args = parser.parse_args()

    build_scripts(args)

if __name__ == "__main__":
    main()

