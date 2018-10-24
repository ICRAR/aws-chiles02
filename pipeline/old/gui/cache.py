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


class Cache:
    """
    Represents a single entry in the cache.
    """
    def __init__(self):
        self.in_use = 0
        self.cached = []

    def get(self, create):
        """
        Get an item from the cache. A new one will be created if the the all the cached items are in use.
        :param create: A function to create a new object if the cache is empty.
        :return:
        """
        if self.in_use == len(self.cached):
            # Create a new item if we've used all our cached items
            self.cached.append(create())

        cached = self.cached[self.in_use]
        self.in_use += 1

        return cached

    def return_all(self, cleanup):
        """
        Return all items to the cache
        :param cleanup: A function to cleanup each object in the cache
        :return:
        """
        for index in range(0, self.in_use):
            cleanup(self.cached[index])

        self.in_use = 0
