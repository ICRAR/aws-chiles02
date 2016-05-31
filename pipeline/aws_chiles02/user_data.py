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
The user data we use
"""
import base64
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from os.path import dirname, join

from mako.lookup import TemplateLookup

from aws_chiles02.settings_file import QUEUE, AWS_REGION

LOG = logging.getLogger(__name__)


def get_user_data(cloud_init_data):
    user_data = MIMEMultipart()
    for cloud_init in cloud_init_data:
        user_data.attach(MIMEText(cloud_init))

    encode = user_data.as_string().encode("ascii")
    encoded_data = base64.b64encode(encode).decode('ascii')

    return encoded_data


def get_node_manager_user_data(boto_data, uuid, max_request_size=10):
    here = dirname(__file__)
    user_data = join(here, '../user_data')
    mako_lookup = TemplateLookup(directories=[user_data])
    template = mako_lookup.get_template('dfms_cloud_init.yaml')
    cloud_init = template.render(
        profile='aws-chiles02',
        aws_access_key_id=boto_data[0],
        aws_secret_access_key=boto_data[1],
        type='node manager',
    )

    template = mako_lookup.get_template('node_manager_start_up.bash')
    user_script = template.render(
        uuid=uuid,
        queue=QUEUE,
        region=AWS_REGION,
        max_request_size=max_request_size,
    )

    user_data = get_user_data([cloud_init, user_script])
    return user_data


def get_data_island_manager_user_data(boto_data, hosts, uuid, max_request_size=10):
    here = dirname(__file__)
    user_data = join(here, '../user_data')
    mako_lookup = TemplateLookup(directories=[user_data])
    template = mako_lookup.get_template('dfms_cloud_init.yaml')
    cloud_init = template.render(
        profile='aws-chiles02',
        aws_access_key_id=boto_data[0],
        aws_secret_access_key=boto_data[1],
        type='island manager',
    )

    template = mako_lookup.get_template('island_manager_start_up.bash')
    user_script = template.render(
        hosts=hosts,
        uuid=uuid,
        queue=QUEUE,
        region=AWS_REGION,
        max_request_size=max_request_size,
    )
    user_data = get_user_data([cloud_init, user_script])
    return user_data
