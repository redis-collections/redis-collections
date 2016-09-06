# -*- coding: utf-8 -*-
"""
syncable
~~~~~

Persistent Python collections that can be manually synchronized to and
retrieved from Redis.

The collections are kept in memory, so their operations run as fast as their
standard counterparts'. When their :func:`sync` method is called their contents
are written to Redis.
"""
from __future__ import division, print_function, unicode_literals

import collections

from .dicts import Counter, DefaultDict, Dict
from .lists import List
from .sets import Set


class _SyncableBase(object):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.sync()


class SyncableDict(_SyncableBase, dict):
    def __init__(self, redis=None, key=None):
        self.persistence = Dict(redis=redis, key=key)
        self.key = self.persistence.key

        super(SyncableDict, self).__init__()
        self.update(self.persistence)

    def sync(self):
        self.persistence.update(self)


class SyncableCounter(_SyncableBase, collections.Counter):
    def __init__(self, redis=None, key=None):
        self.persistence = Counter(redis=redis, key=key)
        self.key = self.persistence.key

        super(SyncableCounter, self).__init__()
        self.update(self.persistence)

    def sync(self):
        self.persistence.clear()
        self.persistence.update(self)


class SyncableDefaultDict(_SyncableBase, collections.defaultdict):
    def __init__(self, default_factory=None, redis=None, key=None):
        self.persistence = DefaultDict(default_factory, redis=redis, key=key)
        self.key = self.persistence.key

        super(SyncableDefaultDict, self).__init__(default_factory)
        self.update(self.persistence)

    def sync(self):
        self.persistence.update(self)


class SyncableList(_SyncableBase, list):
    def __init__(self, redis=None, key=None):
        self.persistence = List(redis=redis, key=key)
        self.key = self.persistence.key

        super(SyncableList, self).__init__()
        self.extend(self.persistence)

    def sync(self):
        self.persistence.clear()
        self.persistence.extend(self)


class SyncableSet(_SyncableBase, set):
    def __init__(self, redis=None, key=None):
        self.persistence = Set(redis=redis, key=key)
        self.key = self.persistence.key

        super(SyncableSet, self).__init__()
        self.update(self.persistence)

    def sync(self):
        self.persistence.clear()
        self.persistence.update(self)
