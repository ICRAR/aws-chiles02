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
from abc import *


class BaseAPI(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def graph_create(self, **kwargs):
        pass

    @abstractmethod
    def graph_use(self, **kwargs):
        pass

    @abstractmethod
    def graph_json(self, **kwargs):
        pass

    @abstractmethod
    def imageconcat_create(self, **kwargs):
        pass

    @abstractmethod
    def imageconcat_use(self, **kwargs):
        pass

    @abstractmethod
    def imageconcat_json(self, **kwargs):
        pass

    @abstractmethod
    def jpeg2000_create(self, **kwargs):
        pass

    @abstractmethod
    def jpeg2000_use(self, **kwargs):
        pass

    @abstractmethod
    def jpeg2000_json(self, **kwargs):
        pass

    @abstractmethod
    def mstransform_create(self, **kwargs):
        pass

    @abstractmethod
    def mstransform_use(self, **kwargs):
        pass

    @abstractmethod
    def mstransform_json(self, **kwargs):
        pass

    @abstractmethod
    def uvsub_create(self, **kwargs):
        pass

    @abstractmethod
    def uvsub_use(self, **kwargs):
        pass

    @abstractmethod
    def uvsub_json(self, **kwargs):
        pass


class NullAPI(BaseAPI):
    def graph_create(self, **kwargs):
        print "Graph Create"
        print kwargs

    def graph_use(self, **kwargs):
        print "Graph Use"
        print kwargs

    def graph_json(self, **kwargs):
        print "Graph Json"
        print kwargs

    def imageconcat_create(self, **kwargs):
        print "Imageconcat Create"
        print kwargs

    def imageconcat_use(self, **kwargs):
        print "Imageconcat Use"
        print kwargs

    def imageconcat_json(self, **kwargs):
        print "Imageconcat Json"
        print kwargs

    def jpeg2000_create(self, **kwargs):
        print "JPEG2000 Create"
        print kwargs

    def jpeg2000_use(self, **kwargs):
        print "JPEG2000 Use"
        print kwargs

    def jpeg2000_json(self, **kwargs):
        print "JPEG2000 Json"
        print kwargs

    def mstransform_create(self, **kwargs):
        print "Mstransform Create"
        print kwargs

    def mstransform_use(self, **kwargs):
        print "Mstransform Use"
        print kwargs

    def mstransform_json(self, **kwargs):
        print "Mstransform Json"
        print kwargs

    def uvsub_create(self, **kwargs):
        print "UVSub Create"
        print kwargs

    def uvsub_use(self, **kwargs):
        print "UVSub Use"
        print kwargs

    def uvsub_json(self, **kwargs):
        print "UVSub Json"
        print kwargs


class ChilesAPI(BaseAPI):
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
        create_and_generate(kwargs['bucket_name'],
                            kwargs['frequency_width'],
                            kwargs['ami_id'],
                            kwargs['spot_price1'],
                            kwargs['spot_price2'],
                            kwargs['volume'],
                            kwargs['days_per_node'],
                            kwargs['add_shutdown'],
                            kwargs['use_bash'],
                            kwargs['casa_version'],
                            kwargs['split_directory'],
                            kwargs['observation_phase'])

    def mstransform_use(self, **kwargs):
        from generate_mstransform_graph import use_and_generate
        use_and_generate(kwargs['host'],
                         kwargs['port'],
                         kwargs['bucket_name'],
                         kwargs['frequency_width'],
                         kwargs['volume'],
                         kwargs['add_shutdown'],
                         kwargs['use_bash'],
                         kwargs['split_directory'],
                         kwargs['observation_phase'],
                         kwargs['casa_version'])

    def mstransform_json(self, **kwargs):
        from generate_mstransform_graph import build_json
        build_json(kwargs['bucket'],
                   kwargs['width'],
                   kwargs['volume'],
                   kwargs['nodes'],
                   kwargs['parallel_streams'],
                   kwargs['add_shutdown'],
                   kwargs['use_bash'],
                   kwargs['split_directory'],
                   kwargs['observation_phase'],
                   kwargs['casa_version'],
                   kwargs['json_path'])

    def uvsub_create(self, **kwargs):
        from generate_uvsub_graph import create_and_generate
        create_and_generate(**kwargs)

    def uvsub_use(self, **kwargs):
        from generate_uvsub_graph import use_and_generate
        use_and_generate(**kwargs)

    def uvsub_json(self, **kwargs):
        from generate_uvsub_graph import generate_json
        generate_json(**kwargs)


# Some functions have different names for the same parameters, so we remap them here


def clean_graph_create(api, **kwargs):
    argument_remap = kwargs.copy()
    argument_remap['arcsec'] = kwargs['arc_seconds']
    argument_remap['robust'] = kwargs['clean_robust']
    argument_remap['spot_price'] = kwargs['spot_price_i3.4xlarge']
    api.graph_create(**argument_remap)


def clean_graph_use(api, **kwargs):
    argument_remap = kwargs.copy()
    argument_remap['host'] = kwargs['dim_ip']
    argument_remap['port'] = kwargs['dim_port']
    argument_remap['arcsec'] = kwargs['arc_seconds']
    argument_remap['robust'] = kwargs['clean_robust']
    api.graph_use(**argument_remap)


def clean_graph_json(api, **kwargs):
    argument_remap = kwargs.copy()
    argument_remap['arcsec'] = kwargs['arc_seconds']
    argument_remap['robust'] = kwargs['clean_robust']
    api.graph_json(**argument_remap)


def imageconcat_create(api, **kwargs):
    argument_remap = kwargs.copy()
    argument_remap['spot_price'] = kwargs['spot_price_i3.xlarge']
    api.imageconcat_create(**argument_remap)


def imageconcat_use(api, **kwargs):
    argument_remap = kwargs.copy()
    argument_remap['host'] = kwargs['dim_ip']
    argument_remap['port'] = kwargs['dim_port']
    api.imageconcat_use(**argument_remap)


def imageconcat_json(api, **kwargs):
    api.imageconcat_json(**kwargs)


def jpeg2000_create(api, **kwargs):
    argument_remap = kwargs.copy()
    argument_remap['spot_price'] = kwargs['spot_price_i3.2xlarge']
    api.jpeg2000_create(**argument_remap)
    pass


def jpeg2000_use(api, **kwargs):
    argument_remap = kwargs.copy()
    argument_remap['host'] = kwargs['dim_ip']
    argument_remap['port'] = kwargs['dim_port']
    api.jpeg2000_use(**argument_remap)
    pass


def jpeg2000_json(api, **kwargs):
    argument_remap = kwargs.copy()
    argument_remap['bucket'] = kwargs['bucket_name']
    argument_remap['shutdown'] = kwargs['add_shutdown']
    api.jpeg2000_json(**argument_remap)
    pass


def mstransform_create(api, **kwargs):
    api.mstransform_create(bucket_name=kwargs['bucket_name'],
                           frequency_width=kwargs['frequency_width'],
                           ami_id=kwargs['ami_id'],
                           spot_price1=kwargs['spot_price_i3.2xlarge'],
                           spot_price2=kwargs['spot_price_i3.4xlarge'],
                           volume=kwargs['volume'],
                           days_per_node=kwargs['days_per_node'],
                           add_shutdown=kwargs['add_shutdown'],
                           use_bash=kwargs['use_bash'],
                           casa_version=kwargs['casa_version'],
                           split_directory=kwargs['split_directory_name'],
                           observation_phase=kwargs['observation_phase'])


def mstransform_use(api, **kwargs):
    api.mstransform_use(host=kwargs['dim_ip'],
                        port=kwargs['dim_port'],
                        bucket_name=kwargs['bucket_name'],
                        frequency_width=kwargs['frequency_width'],
                        volume=kwargs['volume'],
                        add_shutdown=kwargs['add_shutdown'],
                        use_bash=kwargs['use_bash'],
                        split_directory=kwargs['split_directory_name'],
                        observation_phase=kwargs['observation_phase'],
                        casa_version=kwargs['casa_version'])


def mstransform_json(api, **kwargs):
    api.mstransform_json(bucket=kwargs['bucket_name'],
                         width=kwargs['frequency_width'],
                         volume=kwargs['volume'],
                         nodes=kwargs['nodes'],
                         parallel_streams=kwargs['parallel_streams'],
                         add_shutdown=kwargs['add_shutdown'],
                         use_bash=kwargs['use_bash'],
                         split_directory=kwargs['split_directory_name'],
                         observation_phase=kwargs['observation_phase'],
                         casa_version=kwargs['casa_version'],
                         json_path=kwargs['json_path'])


def uvsub_create(api, **kwargs):
    argument_remap = kwargs.copy()
    argument_remap['spot_price'] = kwargs['spot_price_i3.2xlarge']
    argument_remap['split_directory'] = kwargs['split_directory_name']
    api.uvsub_create(**argument_remap)


def uvsub_use(api, **kwargs):
    argument_remap = kwargs.copy()
    argument_remap['host'] = kwargs['dim_ip']
    argument_remap['port'] = kwargs['dim_port']
    argument_remap['split_directory'] = kwargs['split_directory_name']
    api.uvsub_use(**argument_remap)


def uvsub_json(api, **kwargs):
    argument_remap = kwargs.copy()
    argument_remap['bucket'] = kwargs['bucket_name']
    argument_remap['shutdown'] = kwargs['add_shutdown']
    argument_remap['width'] = kwargs['frequency_width']
    argument_remap['split_directory'] = kwargs['split_directory_name']
    api.uvsub_json(**argument_remap)


commands = {
    "Clean Graph": {
        "Create": clean_graph_create,
        "Use": clean_graph_use,
        "Generate JSON": clean_graph_json
    },

    "Image Concatenate": {
        "Create": imageconcat_create,
        "Use": imageconcat_use,
        "Generate JSON": imageconcat_json
    },

    "JPEG2000 Graph": {
        "Create": jpeg2000_create,
        "Use": jpeg2000_use,
        "Generate JSON": jpeg2000_json
    },

    "MSTransform Graph": {
        "Create": mstransform_create,
        "Use": mstransform_use,
        "Generate JSON": mstransform_json
    },

    "UVSub Graph": {
        "Create": uvsub_create,
        "Use": uvsub_use,
        "Generate JSON": uvsub_json
    }
}


def api_command(api, task, action, parameters):
    try:
        command = commands[task][action]
    except KeyError:
        print "Unknown api command {0} {1}".format(task, action)
        return

    try:
        command(api, **parameters)
    except Exception as e:
        print "Couldn't run api command {0} {1}: {2}".format(task, action, e.message)