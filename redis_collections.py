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

    max_key_creation_attempts = 3

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
        self.redis = redis or self._redis_factory()
        self.pickler = pickler or pickle  # standard pickle module as default
        self.prefix = prefix

        if id:
            # summoning existing collection
            self.key, self.id = self._key_factory(id=str(id), prefix=prefix)
        else:
            self.key, self.id = self._key_factory(prefix=prefix)

    def _id_factory(self):
        """Creates default collection ID using :mod:`uuid`.

        :rtype: string
        """
        return str(uuid4().hex)

    def _redis_factory(self):
        """Creates default Redis connection.

        :rtype: :class:`redis.StrictRedis`
        """
        return redis.StrictRedis()

    def _key_factory(self, id=None, prefix=None):
        """Creates Redis key for the collection. If ID is not provided, factory
        searches only for a key which is not ocuppied yet. If it cannot be
        found, :exc:`RuntimeError` is raised.

        :param id: Collection ID.
        :type id: string or None
        :param prefix: Key prefix to use when working with Redis.
        :type prefix: string or None
        :rtype: tuple of strings (key, id)
        """
        def create_key(id):
            components = [prefix, '_redis_collections', id]
            return '.'.join(filter(None, components))

        if id:
            return (create_key(id), id)

        for attempt in range(0, self.max_key_creation_attempts):
            id = self._id_factory()
            key = create_key(id)

            # repeat until an empty key is not found
            if not self.redis.exists(key):
                return (key, id)

        raise RuntimeError('Unable to find a free Redis key.')

    def _pickle(self, data):
        """Converts given data to string.

        :param data: Data to be serialized.
        :type data: anything serializable
        :rtype: string
        """
        return self.pickler.dumps(data)

    def _unpickle(self, string):
        """Converts given string serialization back to corresponding data.
        If ``None`` or empty string given, ``None`` is returned.

        :param string: String to be unserialized.
        :type string: string
        :rtype: anything serializable or None
        """
        if string is None or string == '':
            return None
        if not isinstance(string, basestring):
            msg = 'Only strings can be unpickled (%r given).' % string
            raise TypeError(msg)
        return self.pickler.loads(string)


class Dict(RedisCollection, collections.MutableMapping):
    # http://docs.python.org/2/library/stdtypes.html#mapping-types-dict
    # http://docs.python.org/2/reference/datamodel.html#emulating-container-types
    # http://redis.io/commands#hash

    def __init__(self, iterable=None, **kwargs):
        super(Dict, self).__init__(**kwargs)
        if iterable:
            mapping = dict(iterable)  # conversion to mapping
            keys = mapping.keys()
            values = map(self._pickle, mapping.values())  # pickling values
            self.redis.hmset(self.key, dict(zip(keys, values)))

    def __len__(self):
        return self.redis.hlen(self.key)

    def __iter__(self):
        return self.iterkeys()

    def __contains__(self, key):
        return self.redis.hexists(self.key, key)

    def get(self, key, default=None):
        value = self.redis.hget(self.key, key)
        return self._unpickle(value) or default

    def __getitem__(self, key):
        pipe = self.redis.pipeline()
        pipe.hexists(self.key, key)
        pipe.hget(self.key, key)
        exists, value = pipe.execute()
        if not exists:
            raise KeyError(key)
        return self._unpickle(value)

    def __setitem__(self, key, value):
        value = self._pickle(value)
        self.redis.hset(self.key, key, value)

    def __delitem__(self, key):
        self.redis.hdel(self.key, key)

    def items(self):
        result = self.redis.hgetall(self.key).items()
        return [(k, self._unpickle(v)) for (k, v) in result]

    def iteritems(self):
        result = self.redis.hgetall(self.key).iteritems()
        return ((k, self._unpickle(v)) for (k, v) in result)

    def keys(self):
        return self.redis.hkeys(self.key)

    def iter(self):
        return self.iterkeys()

    def iterkeys(self):
        return iter(self.redis.hkeys(self.key))

    def values(self):
        result = self.redis.hvals(self.key)
        return [self._unpickle(v) for v in result]

    def itervalues(self):
        result = iter(self.redis.hvals(self.key))
        return (self._unpickle(v) for v in result)

    def clear(self):
        self.redis.delete(self.key)

    def copy(self):
        return Dict(self, redis=self.redis, pickler=self.pickler,
                    prefix=self.prefix)

    # def pop(self, key, default=None):
    #     # http://hg.python.org/cpython/file/2.7/Lib/_abcoll.py#l456

    # def popitem(self):
    #     # http://hg.python.org/cpython/file/2.7/Lib/_abcoll.py#l467

    # def setdefault(self, key, default=None):
    #     # http://hg.python.org/cpython/file/2.7/Lib/_abcoll.py#l504
    #     # http://redis.io/commands/hsetnx

    # def update(self, other):
    #     # http://hg.python.org/cpython/file/2.7/Lib/_abcoll.py#l483
    #     # http://redis.io/commands/hmset

    @classmethod
    def fromkeys(cls, seq, value=None, **kwargs):
        iterable = ((item, value) for item in seq)
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
