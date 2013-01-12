# -*- coding: utf-8 -*-
"""
sorted_set
~~~~~~~~~~
"""


import collections

from .base import RedisCollection


class SortedSet(RedisCollection, collections.MutableSet):
    """Mutable **sorted set** collection aiming to have the same API as the
    standard set type. See `set
    <http://docs.python.org/2/library/stdtypes.html#set>`_ for
    further details. The Redis implementation is based on the
    `sorted set <http://redis.io/commands#sorted_set>`_ type.
    """

    # http://code.activestate.com/recipes/576694/

    def __init__(self):
        pass

    def __len__(self):
        pass

    def __iter__(self):
        pass

    def __contains__(self, elem):
        pass

    def add(self, elem):
        pass

    def discard(self, elem):
        pass
