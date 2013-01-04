# -*- coding: utf-8 -*-
"""
redis_collections
~~~~~~~~~~~~~~~~~

Set of basic Python collections backed by Redis.
"""


import redis
import collections
from abc import ABCMeta, abstractmethod
from uuid import uuid4

try:
    import cPickle as pickle
except ImportError:
    import pickle as pickle


class RedisCollection:
    """Abstract class providing backend functionality for all Redis
    collections.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, redis=None, id=None, pickler=None, prefix=None):
        """Initializes the collection.

        :param redis: Redis client instance. If not provided, default Redis
                      connection is used.
        :type redis: :class:`redis.StrictRedis` or None
        :param id: ID of the collection. Collections with the same IDs point
                   to the same data. If not provided, default random ID string
                   is generated.
        :type id: str or None
        :param pickler: Implementation of data serialization. Object with two
                        methods is expected: :func:`dumps` for conversion
                        of data to string and :func:`loads` for the opposite
                        direction. Examples::

                            import json, pickle
                            Dict(pickler=json)
                            Dict(pickler=pickle)  # default

                        Of course, you can construct your own pickling object
                        (it can be class, module, whatever). Default
                        serialization implementation uses :mod:`pickle`.
        :param prefix: Key prefix to use when working with Redis. Default is
                       empty string.
        :type prefix: str or None
        """
        self._redis = redis
        self.pickler = pickler or pickle

        self.id = str(id) if id else self._id_factory()
        self.prefix = prefix
        self.key = self._key_factory()

    def _id_factory(self):
        """Creates default collection ID using :mod:`uuid`.

        :rtype: str
        """
        return str(uuid4().hex)

    def _redis_factory(self):
        """Creates default Redis connection.

        :rtype: :class:`redis.StrictRedis`
        """
        return redis.StrictRedis()

    def _key_factory(self):
        """Creates Redis key for the collection.

        :rtype: str
        """
        # long keys does not matter: http://stackoverflow.com/a/6322977/325365
        components = [self.prefix, '_redis_collections', self.id]
        return (filter(None, components)).join('.')

    @property
    def redis(self):
        """Lazy access to Redis connection. If not set by :func:`__init__`,
        default Redis connection is created.

        :rtype: :class:`redis.StrictRedis`
        """
        if not self._redis:
            self._redis = self._redis_factory()
        return self._redis

    def _pickle(self, data):
        """Converts given data to string.

        :param data: Data to be serialized.
        :type data: anything serializable
        :rtype: str
        """
        return self.pickler.dumps(data)

    def _unpickle(self, string):
        """Converts given string serialization back to corresponding data.
        If ``None`` or empty string given, ``None`` is returned.

        :param string: String to be unserialized.
        :type string: str
        :rtype: anything serializable or None
        """
        if string is None or string == '':
            return None
        return self.pickler.loads(string)


class Dict(RedisCollection, collections.MutableMapping):
    # http://docs.python.org/2/library/stdtypes.html#mapping-types-dict
    # http://docs.python.org/2/reference/datamodel.html#emulating-container-types
    # http://redis.io/commands#hash

    def __init__(self, iterable, **kwargs):
        super(Dict, self).__init__(**kwargs)

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

    @classmethod
    def fromkeys(cls, seq, value=None, **kwargs):
        iterable = [(item, value) for item in seq]
        return cls(iterable, **kwargs)


class List(RedisCollection, collections.MutableSequence):
    # http://docs.python.org/2/library/functions.html#list
    # http://redis.io/commands#list

    def __init__(self):
        pass

    def __len__(self):
        pass

    def __iter__(self):
        pass

    def __contains__(self, key):
        pass

    def __getitem__(self, key):
        pass


class Set(RedisCollection, collections.MutableSet):
    # http://docs.python.org/2/library/stdtypes.html#set
    # http://redis.io/commands#set

    def __init__(self):
        pass

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


class SortedSet(RedisCollection, collections.MutableSet):
    # http://docs.python.org/2/library/stdtypes.html#set
    # http://redis.io/commands#sorted_set

    def __init__(self):
        pass

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
