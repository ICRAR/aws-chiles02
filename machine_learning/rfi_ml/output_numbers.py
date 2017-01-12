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
Output numbers in human readable formats
"""

SYMBOLS = {
    'customary': ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_ext': ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa', 'zetta', 'iotta'),
    'iec': ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'iec_ext': ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi', 'zebi', 'yobi'),
}


def bytes2human(n, format_string='{0:.1f}{1}', symbols='customary'):
    """
    Convert n bytes into a human readable string based on format.
    symbols can be either "customary", "customary_ext", "iec" or "iec_ext",
    see: http://goo.gl/kTQMs

      >>> bytes2human(0)
      '0.0B'
      >>> bytes2human(0.9)
      '0.0B'
      >>> bytes2human(1)
      '1.0B'
      >>> bytes2human(1.9)
      '1.0B'
      >>> bytes2human(1024)
      '1.0K'
      >>> bytes2human(1048576)
      '1.0M'
      >>> bytes2human(1099511627776127398123789121)
      '909.5Y'

      >>> bytes2human(9856, symbols="customary")
      '9.6K'
      >>> bytes2human(9856, symbols="customary_ext")
      '9.6kilo'
      >>> bytes2human(9856, symbols="iec")
      '9.6Ki'
      >>> bytes2human(9856, symbols="iec_ext")
      '9.6kibi'

      >>> bytes2human(10000, "{0:.1f} {1}/sec")
      '9.8 K/sec'

      >>> # precision can be adjusted by playing with %f operator
      >>> bytes2human(10000, format_string="{0:.5f} {1}")
      '9.76562 K'
    """
    n = int(n)
    if n < 0:
        raise ValueError("n < 0")

    symbols = SYMBOLS[symbols]
    prefix = {}
    for i, s in enumerate(symbols[1:]):
        prefix[s] = 1 << (i + 1) * 10
    for symbol in reversed(symbols[1:]):
        if n >= prefix[symbol]:
            value = float(n) / prefix[symbol]
            return format_string.format(value, symbol)
    return format_string.format(n, symbols[0])


def human2bytes(input_string):
    """
    Attempts to guess the string format based on default symbols
    set and return the corresponding bytes as an integer.
    When unable to recognize the format ValueError is raised.

      >>> human2bytes('0 B')
      0
      >>> human2bytes('1 K')
      1024
      >>> human2bytes('1M')
      1048576
      >>> human2bytes('1 Gi')
      1073741824
      >>> human2bytes('1 tera')
      1099511627776
      >>> human2bytes('0.5kilo')
      512
      >>> human2bytes('0.1  byte')
      0
      >>> human2bytes('1 k')  # k is an alias for K
      1024
      >>> human2bytes('12 foo')
      Traceback (most recent call last):
          ...
      ValueError: can't interpret '12 foo'
    """
    init = input_string
    num = ""
    while input_string and input_string[0:1].isdigit() or input_string[0:1] == '.':
        num += input_string[0]
        input_string = input_string[1:]
    num = float(num)
    letter = input_string.strip()
    for name, sset in SYMBOLS.items():
        if letter in sset:
            break
    else:
        if letter == 'k':
            # treat 'k' as an alias for 'K' as per: http://goo.gl/kTQMs
            sset = SYMBOLS['customary']
            letter = letter.upper()
        else:
            raise ValueError("can't interpret '{0}'".format(init))

    prefix = {
        sset[0]: 1
    }
    for i, input_string in enumerate(sset[1:]):
        prefix[input_string] = 1 << (i + 1) * 10
    return int(num * prefix[letter])
