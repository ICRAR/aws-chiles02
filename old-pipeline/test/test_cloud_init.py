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
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from common import get_cloud_init, get_script
from ec2_helper import EC2Helper
from settings_file import AWS_INSTANCES, AWS_AMI_ID, BASH_SCRIPT_SETUP_DISKS

# Setup the MIME
user_data = MIMEMultipart()
user_data.attach(get_cloud_init())

user_data.attach(MIMEText(get_script(BASH_SCRIPT_SETUP_DISKS)))
data_as_string = user_data.as_string()

# Start the EC2 instance
ec2_helper = EC2Helper()
ec2_helper.run_spot_instance(
    AWS_AMI_ID,
    0.1,
    data_as_string,
    'm3.medium',
    None,
    'Kevin',
    'Cloud-Init-Test',
    AWS_INSTANCES['m3.medium'],
    ephemeral=True)
