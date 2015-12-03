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
Use boto to delete a volume
"""
import argparse

from ec2_helper import EC2Helper


def delete_volumes(volume_ids, force, aws_access_key_id=None, aws_secret_access_key=None):
    ec2_helper = EC2Helper(aws_access_key_id, aws_secret_access_key)
    for volume_id in volume_ids:
        ec2_helper.delete_volume(volume_id)


def main():
    parser = argparse.ArgumentParser('Delete a volume')
    parser.add_argument('-f', '--force', action="store_true", help='force the delete')
    parser.add_argument('--aws_access_key_id', help='your aws_access_key_id')
    parser.add_argument('--aws_secret_access_key', help='your aws_secret_access_key')
    parser.add_argument('volume_ids', nargs='+', help='the volume ids')

    args = vars(parser.parse_args())
    delete_volumes(args['volume_ids'], args['force'], args['aws_access_key_id'], args['aws_secret_access_key'])

if __name__ == "__main__":
    main()
