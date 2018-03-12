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
Build the read me HTML file
"""
import argparse
import json
import logging
import time

import boto3
from MarkupPy.markup import escape, page, oneliner
from boto3.s3.transfer import S3Transfer
from os.path import getsize

from aws_chiles02.common import ProgressPercentage, set_logging_level
from aws_chiles02.settings_file import WEB_SITE

LOG = logging.getLogger(__name__)


def build_key(key):
    return 'http://{}/?prefix={}/'.format(WEB_SITE, key)


def build_file(bucket_name):
    session = boto3.Session(profile_name='aws-chiles02')
    client = session.client('s3')

    top_levels = []
    paginator = client.get_paginator('list_objects')
    for result in paginator.paginate(Bucket=bucket_name, Delimiter='/'):
        for prefix in result.get('CommonPrefixes'):
            prefix_text = prefix.get('Prefix')
            if prefix_text.endswith('/'):
                prefix_text = prefix_text[:-1]
            top_levels.append(prefix_text)

    json_files = []
    s3 = session.resource('s3', use_ssl=False)
    bucket = s3.Bucket(bucket_name)
    for top_level in top_levels:
        for key in bucket.objects.filter(Prefix='{0}/aa_parameter_data.json'.format(top_level)):
            json_files.append(key.key)

    json_strings = {}
    for json_file in json_files:
        LOG.info('Getting {}'.format(json_file))

        temporary_filename = '/tmp/parameter_data.json'
        s3_object = s3.Object(bucket_name, json_file)
        s3_size = s3_object.content_length
        s3_client = s3.meta.client
        transfer = S3Transfer(s3_client)
        transfer.download_file(
            bucket_name,
            json_file,
            temporary_filename,
            callback=ProgressPercentage(
                json_file,
                s3_size
            )
        )

        with open(temporary_filename, 'r') as json_fp:
            json_string = json.load(json_fp)
            elements = json_file.split('/')
            run_type = elements[0].split('_')[0].lower()

            if run_type in json_strings.keys():
                run_type_dictionary = json_strings[run_type]
            else:
                run_type_dictionary = {}
                json_strings[run_type] = run_type_dictionary
            run_type_dictionary[elements[0]] = json_string

    # Now write the HTML
    html_page = page()
    html_page.init(
        title='CHILES Readme',
        header='<h1>The CHILES runs and what they are</h1>',
        footer='Generated: {}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    )
    html_page.p(('The files in this directory are those generated by the CHILES pipeline using AWS. '
                 'They are the <i>final</i> image cube for CHILES.',
                 'We say <i>final</i> as for every improvement (mainly in MODELS recently, but we will '
                 'also improve the FLAGGING) we will regenerate the image cube and the intermediate products.',
                 'These products are:'))
    html_page.ul()
    html_page.li('the SPLIT data (into 4MHz sub-bands)')
    html_page.li('the UVSUBtracted data (from continuum models of the whole dataset)')
    html_page.li('the CLEAN image cubes (into 4MHz sub-bands)')
    html_page.li('the IMAGECONCAT image cubes (into 24MHz sub-bands, in steps of 20MHz)')
    html_page.ul.close()

    html_page.p(('Each stage is in a subdirectory with a data string and in most cases '
                 'some additional information to indicate what was the input for the '
                 'processing. i.e. TASK_DATESTR[_MODE]',
                 'In all cases there is file aa_parameter_data.json which holds all the '
                 'processing parameters in the sub-directory. '
                 'This file used to generate this document.',
                 'If the directory is empty this usually indicates the AWS spot price rose above '
                 'the threshold we had set and the run was terminated.'))

    if 'split' in json_strings.keys():
        html_page.h1('Split')
        html_page.table(border='1px solid black')
        html_page.tr()
        html_page.th(('Name', 'Value'))
        html_page.tr.close()
        dictionary = json_strings['split']
        for key in sorted(dictionary.keys()):
            values = dictionary[key]
            html_page.tr()
            html_page.td((key, 'Value'))
            # TODO:
            html_page.tr.close()
        html_page.table.close()

    if 'uvsub' in json_strings.keys():
        html_page.h1('UVSUB')
        html_page.table(border='1px solid black')
        html_page.tr()
        html_page.th((
            'Name',
            'Run Note',
            'Split Directory',
            'Absorption',
            'Casa',
            'Taylor Terms',
            'W Projection Planes',
            'Produce QA',
            'Scan Statistics',
            'Frequency Width',
            'Session Id'
        ))
        html_page.tr.close()
        dictionary = json_strings['uvsub']
        for key in sorted(dictionary.keys()):
            values = dictionary[key]
            html_page.tr()
            html_page.td((
                oneliner.a(escape(key), href=build_key(key)),
                escape(values.get('run_note', '')),
                escape(values.get('split_directory', '')),
                escape(values.get('absorption', '')),
                escape(values.get('casa_version', '')),
                escape(values.get('number_taylor_terms', '')),
                escape(values.get('w_projection_planes', '')),
                escape(values.get('produce_qa', '')),
                escape(values.get('scan_statistics', '')),
                escape(values.get('width', '')),
                escape(values.get('session_id', ''))
            ))
            html_page.tr.close()
        html_page.table.close()

    if 'clean' in json_strings.keys():
        html_page.h1('Clean')
        html_page.table(border='1px solid black')
        html_page.tr()
        html_page.th((
            'Name',
            'Run Note',
            'Arcsecs',
            'Build Fits',
            'Casa',
            'Clean Channel Average',
            'Clean or TClean',
            'Clean Weighting',
            'Image Size',
            'Iterations',
            'Only Image',
            'Produce QA',
            'Region File',
            'UV Sub Name',
            'W Projection Planes',
            'Frequency Width',
            'Session Id'
        ))
        html_page.tr.close()
        dictionary = json_strings['clean']
        for key in sorted(dictionary.keys()):
            values = dictionary[key]
            html_page.tr()
            clean_weighting_uv = values.get('clean_weighting_uv', '')
            html_page.td((
                oneliner.a(escape(key), href=build_key(key)),
                escape(values.get('run_note', '')),
                escape(values.get('arcsec', '')),
                escape(values.get('Build Fits', '')),
                escape(values.get('casa_version', '4.*')),
                escape(values.get('clean_channel_average', '')),
                escape(values.get('clean_tclean', 'clean')),
                escape(clean_weighting_uv + ': {}'.format(values.get('robust', '')) if clean_weighting_uv == 'briggs' else ''),
                escape(values.get('image_size', '')),
                escape(values.get('iterations', '')),
                escape(values.get('only_image', '')),
                escape(values.get('produce_qa', '')),
                escape(values.get('region_file', '')),
                escape(values.get('uvsub_directory_name', '')),
                escape(values.get('w_projection_planes', '')),
                escape(values.get('width', '')),
                escape(values.get('session_id', ''))
            ))
            html_page.tr.close()
        html_page.table.close()

    if 'imageconcat' in json_strings.keys():
        html_page.h1('IMAGECONCAT')
        html_page.table(border='1px solid black')
        html_page.tr()
        html_page.th((
            'Name',
            'Run Note',
            'CASA',
            'Clean Directory',
            'imageconcat Width',
            'Session Id'
        ))
        html_page.tr.close()
        dictionary = json_strings['imageconcat']
        for key in sorted(dictionary.keys()):
            values = dictionary[key]
            html_page.tr()
            html_page.td((
                oneliner.a(escape(key), href=build_key(key)),
                escape(values.get('run_note', '')),
                escape(values.get('casa_version', '')),
                escape(values.get('clean_directory_name', '')),
                escape(values.get('imageconcat_width', '')),
                escape(values.get('session_id', ''))
            ))
            html_page.tr.close()
        html_page.table.close()

    with open('/tmp/2018_readme.html', 'w') as html_file:
        html_file.write(str(html_page))

    s3_client = s3.meta.client
    transfer = S3Transfer(s3_client)
    transfer.upload_file(
        '/tmp/2018_readme.html',
        bucket_name,
        '2018_readme.html',
        callback=ProgressPercentage(
            '2018_readme.html',
            float(getsize('/tmp/2018_readme.html')),
        ),
        extra_args={
            'StorageClass': 'REDUCED_REDUNDANCY',
            'ContentType': 'text/html'
        }
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser('Build the read me file')
    parser.add_argument('bucket_name', help='the bucket to check')
    args = parser.parse_args()

    set_logging_level(1)
    build_file(args.bucket_name)
