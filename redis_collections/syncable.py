# -*- coding: utf-8 -*-
"""
syncable
~~~~~

Persistent Python collections that can be written to and read from Redis.
The collections are kept in memory, so their operations run as fast as their
standard counterparts'.

Use in a ``with`` block to automatically sync to Redis after the block
executes, or call the :func:`sync` method explicitly.

    >>> with SyncableDict() as D:
    ...     D['one'] = 1
    ...
    >>> D  # Contents are available locally and are stored in Redis
    {'one': 1}
    >>> D['two'] = 2  # Changes are available locally, but not in Redis...
    >>> D.sync()  # ...until sync is called.

If you specify a ``key`` pointing to an existing collection, its contents will
be loaded.

    >>> D.key
    'f4a78a6faacb4d8e97829f9888ac6740'
    >>> E = SyncableDict(key='f4a78a6faacb4d8e97829f9888ac6740')
    >>> E
    {'one': 1, 'two': 2}



"""
from __future__ import division, print_function, unicode_literals

import collections

from .dicts import Counter, DefaultDict, Dict
from .lists import List
from .sets import Set


class _SyncableBase(object):
    @property
    def key(self):
        return self.persistence.key

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.sync()


class SyncableDict(_SyncableBase, dict):
    """
    :class:`dict` subclass whose contents can be synced to Redis.

    See Python's `dict documentation
    <https://docs.python.org/3/library/stdtypes.html#mapping-types-dict>`_ for
    details.
    """

    def __init__(self, **kwargs):
        self.persistence = Dict(**kwargs)

        super(SyncableDict, self).__init__()
        self.update(self.persistence)

    def sync(self):
        self.persistence.update(self)


class SyncableCounter(_SyncableBase, collections.Counter):
    """
    :class:`collections.Counter` subclass whose contents can be synced to
    Redis.

    See Python's `Counter documentation
    <https://docs.python.org/3/library/collections.html#collections.Counter>`_
    for details.
    """

    def __init__(self, **kwargs):
        self.persistence = Counter(**kwargs)

        super(SyncableCounter, self).__init__()
        self.update(self.persistence)

    def sync(self):
        self.persistence.clear()
        self.persistence.update(self)


class SyncableDefaultDict(_SyncableBase, collections.defaultdict):
    """
    :class:`collections.defaultdict` subclass whose contents can be synced to
    Redis.

    See Python's `defaultdict documentation
    <https://docs.python.org/3/library/collections.html
    #collections.defaultdict>`_ for details.
    """

    def __init__(self, *args, **kwargs):
        self.persistence = DefaultDict(*args, **kwargs)

        super(SyncableDefaultDict, self).__init__(args[0] if args else None)
        self.update(self.persistence)

    def sync(self):
        self.persistence.update(self)


class SyncableList(_SyncableBase, list):
    """
    :class:`list` subclass whose contents can be synced to Redis.

    See Python's `list documentation
    <https://docs.python.org/3/library/stdtypes.html
    #sequence-types-list-tuple-range>`_ for details.
    """

    def __init__(self, **kwargs):
        self.persistence = List(**kwargs)

        super(SyncableList, self).__init__()
        self.extend(self.persistence)

    def sync(self):
        self.persistence.clear()
        self.persistence.extend(self)


class SyncableSet(_SyncableBase, set):
    """
    :class:`set` subclass whose contents can be synced to Redis.

    See Python's `set documentation
    <https://docs.python.org/3/library/stdtypes.html
    #set-types-set-frozenset>`_ for details.
    """

    def __init__(self, **kwargs):
        self.persistence = Set(**kwargs)

        super(SyncableSet, self).__init__()
        self.update(self.persistence)

    def sync(self):
        self.persistence.clear()
        self.persistence.update(self)
