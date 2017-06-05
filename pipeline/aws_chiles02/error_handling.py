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
The error handler class
"""
import json

import boto3

from .settings_file import AWS_REGION


class ErrorListener(object):
    def on_error(self, drop):
        """
        Handles the error
        """
        self.send_message(
            drop.error_message,
            drop.session_id,
            drop.oid,
            drop.uid
        )

    @staticmethod
    def send_message(
            message_text,
            session_id,
            oid,
            uid,
            queue='dfms-messages',
            region=AWS_REGION,
            profile_name='aws-chiles02'):
        session = boto3.Session(profile_name=profile_name)
        sqs = session.resource('sqs', region_name=region)
        queue = sqs.get_queue_by_name(QueueName=queue)
        message = {
            'session_id': session_id,
            'uid': uid,
            'oid': oid,
            'message': message_text,
        }
        json_message = json.dumps(message, indent=2)
        queue.send_message(
            MessageBody=json_message,
        )
