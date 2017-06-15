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
A fake logger for CASA
"""
import sys


class CasaLogger(object):
    def __init__(self, origin):
        self._origin = origin

    def info(self, message):
        self._post(message, priority="INFO")

    def warning(self, message):
        self._post(message, priority="WARNING")

    def exception(self, message):
        exception_info = sys.exc_info()
        self._post('{0}\n{1}'.format(message, exception_info), priority="SEVERE")

    def _post(self, message, priority):
        print '{0}:{1}:{2}'.format(self._origin, priority, message)


LOG = CasaLogger(__name__)


def format_arg_value(arg_val):
    """
    Return a string representing a (name, value) pair.

    :param arg_val: the tuple
    """
    arg, val = arg_val
    return '{0}={1}'.format(arg, val)


def name(item):
    """
    Return an item's name.

    :param item: the item
    """
    return item.__name__


def echo(fn):
    """
    Echo calls to a function.

    Returns a decorated version of the input function which "echoes" calls
    made to it by writing out the function's name and the arguments it was
    called with.
    :param fn: the function name
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
        LOG.info('echo: {0}({1},{2},{3},{4})'.format(name(fn), positional, defaulted, nameless, keyword))
        return fn(*v, **k)
    return wrapped
