# -*- coding: utf-8 -*-


# import redis
import collections
# from uuid import uuid4 as uuid


class Dict(collections.MutableMapping):
    # http://docs.python.org/2/library/stdtypes.html#mapping-types-dict
    # http://docs.python.org/2/reference/datamodel.html#emulating-container-types
    # http://docs.python.org/2/library/collections.html#collections-abstract-base-classes
    # http://redis.io/commands#hash

    def __len__(self):
        pass

    def __iter__(self):
        pass

    def __contains__(self, key):
        pass

    def __getitem__(self, key):
        pass

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass


class List(collections.MutableSequence):

    def __len__(self):
        pass

    def __iter__(self):
        pass

    def __contains__(self, key):
        pass

    def __getitem__(self, key):
        pass


class Set(collections.MutableSet):

    def __len__(self):
        pass

    def __iter__(self):
        pass

    def __contains__(self, value):
        pass

    def add(self, value):
        pass

    def discard(self, value):
        pass


class SortedSet(collections.MutableSet):

    def __len__(self):
        pass

    def __iter__(self):
        pass

    def __contains__(self, value):
        pass

    def add(self, value):
        pass

    def discard(self, value):
        pass
