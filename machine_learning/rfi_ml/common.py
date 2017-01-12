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
Common Files
"""
import os

import pandas


def load_files(filenames):
    """
    Load a file and add columns for the min and max frequency
    :param filenames: the filenames to load
    :return: the data frame

    >>> load_files(['/tmp/does_not_exists'])

    >>> data_frame = load_files(['/Users/kevinvinsen/Documents/ICRAR/data/uvsub_4/944_948/stats_13B-266.sb25390589.eb28636346.56672.1890915625.tar.gz'])
    >>> len(data_frame)
    6400
    >>> data_frame.observation.unique()
    array(['13B-266.sb25390589.eb28636346.56672.1890915625'], dtype=object)

    >>> data_frame = load_files(['/Users/kevinvinsen/Documents/ICRAR/data/uvsub_4/944_948/stats_13B-266.sb25390589.eb28636346.56672.1890915625.tar.gz',
    ...                          '/Users/kevinvinsen/Documents/ICRAR/data/uvsub_4/944_948/stats_13B-266.sb25390589.eb28660136.56673.2071233912.tar.gz'])
    >>> len(data_frame)
    12800
    >>> data_frame.observation.unique()
    array(['13B-266.sb25390589.eb28636346.56672.1890915625', '13B-266.sb25390589.eb28660136.56673.2071233912'], dtype=object)

    """
    final_data_frame = None
    for filename in filenames:
        # Check the file exists
        if os.path.exists(filename) and os.path.isfile(filename):
            # Read the data from the CSV file
            data_frame = pandas.read_csv(filename, compression='gzip', header=0, skip_blank_lines=True)
            data_frame.dropna(inplace=True)

            # The first column has the filename in it, so we need to change that
            column_names = data_frame.columns.values.tolist()
            csv_filename = column_names[0]
            data_frame.rename(columns={csv_filename: 'observation'}, inplace=True)

            # Get the minimum and maximum frequency from the filename
            (basename, ext) = os.path.splitext(csv_filename)
            elements = basename.split('_')
            elements = elements[1].split('~')
            minimum_frequency = int(elements[0])
            maximum_frequency = int(elements[1])

            # Add them to the end of the frame
            data_frame['min_freq'] = minimum_frequency
            data_frame['max_freq'] = maximum_frequency

            if final_data_frame is None:
                final_data_frame = data_frame
            else:
                final_data_frame = final_data_frame.append(data_frame, ignore_index=True)

    return final_data_frame
