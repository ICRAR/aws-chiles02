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

from api import BaseChilesAPI
from generate_clean_graph import create_and_generate as clean_create, \
    use_and_generate as clean_use, \
    generate_json as clean_json

from generate_imageconcat import create_and_generate as imageconcat_create, \
    use_and_generate as imageconcat_use, \
    generate_json as imageconcat_json

from generate_jpeg2000_graph import create_and_generate as jpeg2000_create, \
    use_and_generate as jpeg2000_use, \
    save_json as jpeg2000_json

from generate_mstransform_graph import create_and_generate as mstransform_create, \
    use_and_generate as mstransform_use, \
    build_json as mstransform_json

from generate_uvsub_graph import create_and_generate as uvsub_create, \
    use_and_generate as uvsub_use, \
    generate_json as uvsub_json


commands = {
    "Clean Graph": {
        "Create Graph": clean_create,
        "Use Graph": clean_use,
        "Generate JSON": clean_json
    },

    "Image Concatenate": {
        "Create Graph": imageconcat_create,
        "Use Graph": imageconcat_use,
        "Generate JSON": imageconcat_json
    },

    "JPEG2000 Graph": {
        "Create Graph": jpeg2000_create,
        "Use Graph": jpeg2000_use,
        "Generate JSON": jpeg2000_json
    },

    "MSTransform Graph": {
        "Create Graph": mstransform_create,
        "Use Graph": mstransform_use,
        "Generate JSON": mstransform_json
    },

    "UVSub Graph": {
        "Create Graph": uvsub_create,
        "Use Graph": uvsub_use,
        "Generate JSON": uvsub_json
    }
}


class ChilesAPI(BaseChilesAPI):

    def command(self, task, action, parameters):
        try:
            commands[task][action](**parameters)
        except KeyError:
            print "Unknown command {0} : {1}".format(task, action)
