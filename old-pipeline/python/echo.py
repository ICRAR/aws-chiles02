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
Echo the arguments passed to a function
"""
import collections
import inspect
from types import InstanceType, NoneType


def format_arg_value(arg_val):
    """ Return a string representing a (name, value) pair.

    >>> format_arg_value(('x', (1, 2, 3)))
    'x=(1, 2, 3)'
    """
    arg, val = arg_val
    return '{0}={1}'.format(arg, val)


def name(item):
    """ Return an item's name."""
    return item.__name__


def echo(fn):
    """ Echo calls to a function.

    Returns a decorated version of the input function which "echoes" calls
    made to it by writing out the function's name and the arguments it was
    called with.
    """
    import functools
    # Unpack function's arg count, arg names, arg defaults
    code = fn.func_code
    argcount = code.co_argcount
    argnames = code.co_varnames[:argcount]
    fn_defaults = fn.func_defaults or list()
    argdefs = dict(zip(argnames[-len(fn_defaults):], fn_defaults))

    @functools.wraps(fn)
    def wrapped(*v, **k):
        # Collect function arguments by chaining together positional,
        # defaulted, extra positional and keyword arguments.
        positional = map(format_arg_value, zip(argnames, v))
        defaulted = [format_arg_value((a, argdefs[a]))
                     for a in argnames[len(v):] if a not in k]
        nameless = map(repr, v[argcount:])
        keyword = map(format_arg_value, k.items())
        print 'echo: {0}({1},{2},{3},{4})'.format(name(fn), positional, defaulted, nameless, keyword)
        return fn(*v, **k)
    return wrapped


def dump_all():
    stack = inspect.stack()
    caller = stack[1][0]
    caller_vars = caller.f_globals
    caller_vars.update(caller.f_locals)

    print '''##### dump_all start #####
{0}:{1} {2}'''.format(stack[1][1], stack[1][2], stack[1][3])
    ordered_dictionary = collections.OrderedDict(sorted(caller_vars.items()))
    for key, value in ordered_dictionary.iteritems():
        if key == '__builtins__' \
                or key == '__name__' \
                or key == '__file__' \
                or key == '__package__' \
                or key == '__doc__':
            # Ignore this one
            pass
        elif isinstance(value, bool) \
                or isinstance(value, int) \
                or isinstance(value, long) \
                or isinstance(value, float) \
                or isinstance(value, str) \
                or isinstance(value, tuple) \
                or isinstance(value, list) \
                or isinstance(value, dict) \
                or isinstance(value, InstanceType) \
                or isinstance(value, NoneType):
            print '{0}({1}): {2}'.format(key, type(value), value)

    print '''##### dump_all end #####'''
