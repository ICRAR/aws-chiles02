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

# Some functions have different names for the same parameters, so we remap them here

class APINull(object):
    def graph_create(self, **kwargs):
        print "Graph Create"
        print kwargs

    def graph_use(self, **kwargs):
        print "Graph Create"
        print kwargs

    def graph_json(self, **kwargs):
        print "Graph Create"
        print kwargs

    def imageconcat_create(self, **kwargs):
        print "Graph Create"
        print kwargs

    def imageconcat_use(self, **kwargs):
        print "Graph Create"
        print kwargs

    def imageconcat_json(self, **kwargs):
        print "Graph Create"
        print kwargs

    def jpeg2000_create(self, **kwargs):
        print "Graph Create"
        print kwargs

    def jpeg2000_use(self, **kwargs):
        print "Graph Create"
        print kwargs

    def jpeg2000_json(self, **kwargs):
        print "Graph Create"
        print kwargs

    def mstransform_create(self, **kwargs):
        print "Graph Create"
        print kwargs

    def mstransform_use(self, **kwargs):
        print "Graph Create"
        print kwargs

    def mstransform_json(self, **kwargs):
        print "Graph Create"
        print kwargs

    def uvsub_create(self, **kwargs):
        print "Graph Create"
        print kwargs

    def uvsub_use(self, **kwargs):
        print "Graph Create"
        print kwargs

    def uvsub_json(self, **kwargs):
        print "Graph Create"
        print kwargs


class APIFunctions(object):
    def graph_create(self, **kwargs):
        from generate_clean_graph import create_and_generate
        create_and_generate(**kwargs)

    def graph_use(self, **kwargs):
        from generate_clean_graph import use_and_generate
        use_and_generate(**kwargs)

    def graph_json(self, **kwargs):
        from generate_clean_graph import generate_json
        generate_json(**kwargs)

    def imageconcat_create(self, **kwargs):
        from generate_imageconcat import create_and_generate
        create_and_generate(**kwargs)

    def imageconcat_use(self, **kwargs):
        from generate_imageconcat import use_and_generate
        use_and_generate(**kwargs)

    def imageconcat_json(self, **kwargs):
        from generate_imageconcat import generate_json
        generate_json(**kwargs)

    def jpeg2000_create(self, **kwargs):
        from generate_jpeg2000_graph import create_and_generate
        create_and_generate(**kwargs)

    def jpeg2000_use(self, **kwargs):
        from generate_jpeg2000_graph import use_and_generate
        use_and_generate(**kwargs)

    def jpeg2000_json(self, **kwargs):
        from generate_jpeg2000_graph import save_json
        save_json(**kwargs)

    def mstransform_create(self, **kwargs):
        from generate_mstransform_graph import create_and_generate
        create_and_generate(**kwargs)

    def mstransform_use(self, **kwargs):
        from generate_mstransform_graph import use_and_generate
        use_and_generate(**kwargs)

    def mstransform_json(self, **kwargs):
        from generate_mstransform_graph import build_json
        build_json(**kwargs)

    def uvsub_create(self, **kwargs):
        from generate_uvsub_graph import create_and_generate
        create_and_generate(**kwargs)

    def uvsub_use(self, **kwargs):
        from generate_uvsub_graph import use_and_generate
        use_and_generate(**kwargs)

    def uvsub_json(self, **kwargs):
        from generate_uvsub_graph import generate_json
        generate_json(**kwargs)

def clean_graph_create(**kwargs):

    argument_remap = kwargs.copy()
    argument_remap['arcsec'] = kwargs['arc_seconds'] + 'arcsec'
    argument_remap['robust'] = kwargs['clean_robust']
    argument_remap['ami_id'] = kwargs['ami']
    argument_remap['spot_price'] = kwargs['spot_price_i3_4xlarge']
    create_and_generate(**argument_remap)


def clean_graph_use(**kwargs):

    argument_remap = kwargs.copy()
    argument_remap['host'] = kwargs['dim_ip']
    argument_remap['port'] = kwargs['dim_port']
    argument_remap['arcsec'] = kwargs['arc_seconds'] + 'arcsec'
    argument_remap['robust'] = kwargs['clean_robust']
    use_and_generate(**argument_remap)


def clean_graph_json(**kwargs):

    argument_remap = kwargs.copy()
    argument_remap['arcsec'] = kwargs['arc_seconds'] + 'arcsec'
    argument_remap['robust'] = kwargs['clean_robust']
    generate_json(**argument_remap)


def imageconcat_create(**kwargs):

    argument_remap = kwargs.copy()
    argument_remap['spot_price'] = kwargs['spot_price_i3_xlarge']
    create_and_generate(**argument_remap)


def imageconcat_use(**kwargs):

    argument_remap = kwargs.copy()
    argument_remap['host'] = kwargs['dim_ip']
    argument_remap['port'] = kwargs['dim_port']
    use_and_generate(**argument_remap)


def imageconcat_json(**kwargs):

    generate_json(**kwargs)


def jpeg2000_create(**kwargs):

    argument_remap = kwargs.copy()
    argument_remap['spot_price'] = kwargs['spot_price_i3_2xlarge']
    create_and_generate(**argument_remap)
    pass


def jpeg2000_use(**kwargs):

    argument_remap = kwargs.copy()
    argument_remap['host'] = kwargs['dim_ip']
    argument_remap['port'] = kwargs['dim_port']
    use_and_generate(**argument_remap)
    pass


def jpeg2000_json(**kwargs):

    argument_remap = kwargs.copy()
    argument_remap['bucket'] = kwargs['bucket_name']
    argument_remap['shutdown'] = kwargs['add_shutdown']
    save_json(**argument_remap)
    pass


def mstransform_create(**kwargs):

    create_and_generate(kwargs['bucket_name'],
                        kwargs['frequency_width'],
                        kwargs['ami_id'],
                        kwargs['spot_price_i3.2xlarge'],
                        kwargs['spot_price_i3.4xlarge'],
                        kwargs['volume'],
                        kwargs['days_per_node'],
                        kwargs['add_shutdown'],
                        kwargs['use_bash'],
                        kwargs['casa_version'],
                        kwargs['split_directory'],
                        kwargs['observation_phase'])


def mstransform_use(**kwargs):

    use_and_generate(kwargs['dim_ip'],
                     kwargs['dim_port'],
                     kwargs['bucket_name'],
                     kwargs['frequency_width'],
                     kwargs['volume'],
                     kwargs['add_shutdown'],
                     kwargs['use_bash'],
                     kwargs['split_directory'],
                     kwargs['observation_phase'],
                     kwargs['casa_version'])


def mstransform_json(**kwargs):

    build_json(kwargs['bucket_name'],
               kwargs['frequency_width'],
               kwargs['volume'],
               kwargs['nodes'],
               kwargs['parallel_streams'],
               kwargs['add_shutdown'],
               kwargs['use_bash'],
               kwargs['split_directory'],
               kwargs['observation_phase'],
               kwargs['casa_version'])


def uvsub_create(**kwargs):

    argument_remap = kwargs.copy()
    argument_remap['spot_price'] = kwargs['spot_price_i3_2xlarge']
    create_and_generate(**argument_remap)


def uvsub_use(**kwargs):

    argument_remap = kwargs.copy()
    argument_remap['host'] = kwargs['dim_ip']
    argument_remap['port'] = kwargs['dim_port']
    use_and_generate(**argument_remap)


def uvsub_json(**kwargs):

    argument_remap = kwargs.copy()
    argument_remap['width'] = kwargs['frequency_width']
    generate_json(**argument_remap)


commands = {
    "Clean Graph": {
        "Create Graph": clean_graph_create,
        "Use Graph": clean_graph_use,
        "Generate JSON": clean_graph_json
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
