# -*- coding: utf-8 -*-
"""
syncable
~~~~~~~~

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

try:
    import collections.abc as collections_abc
except ImportError:
    import collections as collections_abc

import collections

from .dicts import Counter, DefaultDict, Dict
from .lists import Deque, List
from .sets import Set


class _SyncableBase(object):
    @property
    def redis(self):
        return self.persistence.redis

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
        self.persistence.clear()
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
        self.persistence.clear()
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


class SyncableDeque(_SyncableBase, collections.deque):
    """
    :class:`deque` subclass whose contents can be synced to Redis.

    See Python's `deque documentation
    <https://docs.python.org/3/library/collections.html#collections.deque>`_
    for details.
    """

    def __init__(self, iterable=None, maxlen=None, **kwargs):
        self.persistence = Deque(iterable=iterable, maxlen=maxlen, **kwargs)

        super(SyncableDeque, self).__init__(maxlen=self.persistence.maxlen)
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


class LRUDict(_SyncableBase, collections_abc.MutableMapping):
    """
    :class:`dict`-like class that stores recently-used items in memory
    and automatically pushes older items to Redis.
    Useful for constraining a Python process's memory footprint when a Redis
    server with lots of memory is available.

    >>> D = LRUDict(maxsize=2)
    >>> D['a'] = 1  # 'a' is stored locally
    >>> D['b'] = 2  # 'b' is stored locally
    >>> D['c'] = 2  # 'a' is pushed to Redis and 'c' is stored locally
    >>> assert D['a'] == 1  # 'a' is retrieved from Redis and replaces 'b'
    >>> assert D['b'] == 2  # 'b' is retrieved from Redis and replaces 'c'
    >>> D['b'] = -2
    >>> D.sync()  # All items are copied to Redis
    """

    def __init__(self, maxsize=None, **kwargs):
        """
        :param maxsize: The number of items to keep in memory.
        :type maxsize: integer
        :param redis: Redis client instance. If not provided, default Redis
                      connection is used.
        :type redis: :class:`redis.StrictRedis`
        :param key: Redis key for the collection. Collections with the same key
                    point to the same data. If not provided, a random
                    string is generated.
        :type key: str
        """
        self.maxsize = None if (maxsize is None) else int(maxsize)
        self.cache = collections.OrderedDict()
        self.persistence = Dict(**kwargs)

        super(LRUDict, self).__init__()
        self.update(self.persistence)

    def __contains__(self, key):
        return (key in self.cache) or (key in self.persistence)

    def __delitem__(self, key):
        # Remove the given key from the local cache and the Redis cache
        for D in (self.cache, self.persistence):
            try:
                del D[key]
            except KeyError:
                pass

    def __getitem__(self, key):
        # If the item is in the local cache, grab its value and re-insert it
        # as the rightmost (i.e. most recently used) item.
        # Otherwise, try to get out of Redis and then insert it as the
        # rightmost item.
        try:
            value = self.cache.pop(key)
        except KeyError:
            value = self.persistence.pop(key)

        self[key] = value
        return value

    def __iter__(self):
        local_keys = set(self.cache.keys())
        persistence_keys = set(self.persistence.keys())

        return iter(local_keys | persistence_keys)

    def __len__(self):
        return sum(1 for k in self)

    def __setitem__(self, key, value):
        # If the key is in the local cache, remove it and re-insert it with
        # the new value as the rightmost (i.e. most recently used) item.
        # Otherwise, we'll add it to the local cache. If the local cache
        # is full we'll remove the leftmost (i.e. least recently used) item
        # and insert the new values as the rightmost item.
        try:
            self.cache.pop(key)
        except KeyError:
            if (self.maxsize is not None) and len(self.cache) >= self.maxsize:
                old_key, old_value = self.cache.popitem(last=False)
                self.persistence[old_key] = old_value

        self.cache[key] = value

    def clear(self):
        """
        Removes all items from both the local cache and the collection's Redis
        key.
        """
        self.cache.clear()
        self.persistence.clear()

    def copy(self, key=None):
        """
        Creates another collection with the same items and maxsize with
        the given *key*.
        """
        other = self.__class__(
            maxsize=self.maxsize, redis=self.persistence.redis, key=key
        )
        other.update(self)

        return other

    @classmethod
    def fromkeys(cls, seq, value=None, **kwargs):
        """
        Create a new collection with keys from *seq* and values set to
        *value*. The keyword arguments are passed to the persistent ``Dict``.
        """
        other = cls(**kwargs)
        other.update(((key, value) for key in seq))

        return other

    def sync(self, clear_cache=False):
        """
        Copy items from the local cache to the persistent Dict.
        If *clear_cache* is ``True``, clear out the local cache after
        pushing its items to Redis.
        """
        self.persistence.update(self)

        if clear_cache:
            self.cache.clear()
