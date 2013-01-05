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

    not_impl_msg = ('Cannot be implemented efficiently or atomically '
                    'due to limitations in Redis command set.')

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

    def _create_instance(self, *args, **kwargs):
        kwargs.setdefault('redis', self.redis)
        kwargs.setdefault('pickler', self.pickler)
        kwargs.setdefault('prefix', self.prefix)
        return self.__class__(*args, **kwargs)

    def clear(self):
        self.redis.delete(self.key)

    def __repr__(self):
        cls_name = self.__class__.__name__
        return '<redis_collections.%s %s>' % (cls_name, self.id)


class Dict(RedisCollection, collections.MutableMapping):
    # http://docs.python.org/2/library/stdtypes.html#mapping-types-dict
    # http://redis.io/commands#hash

    __marker = object()

    def __init__(self, values=None, **kwargs):
        super(Dict, self).__init__(**kwargs)
        if values:
            self.update(values)

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

    def copy(self):
        return self._create_instance(values=self)

    def pop(self, key, default=__marker):
        pipe = self.redis.pipeline()
        pipe.hget(self.key, key)
        pipe.hdel(self.key, key)
        value, existed = pipe.execute()

        if not existed:
            if default is self.__marker:
                raise KeyError(key)
            return default
        return self._unpickle(value)

    def popitem(self):
        key = None
        value = None

        # Cannot use self.redis.transaction, because we need to know the key
        # and there is probably not a nice way how to get it out of the
        # closure's scope.
        with self.redis.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(self.key)

                    try:
                        key = pipe.hkeys(self.key)[0]
                    except IndexError:
                        raise KeyError

                    pipe.multi()
                    pipe.hget(self.key, key)
                    pipe.hdel(self.key, key)
                    value, _ = pipe.execute()
                    break

                except redis.WatchError:
                    continue

        return key, self._unpickle(value)

    def setdefault(self, key, default=None):
        pipe = self.redis.pipeline()
        pipe.hsetnx(self.key, key, self._pickle(default))
        pipe.hget(self.key, key)
        _, value = pipe.execute()

        return self._unpickle(value)

    def update(self, *args, **kwargs):
        mapping = {}
        mapping.update(*args, **kwargs)

        keys = mapping.keys()
        values = map(self._pickle, mapping.values())  # pickling values

        self.redis.hmset(self.key, dict(zip(keys, values)))

    @classmethod
    def fromkeys(cls, seq, value=None, **kwargs):
        values = ((item, value) for item in seq)
        return cls(values, **kwargs)


class List(RedisCollection, collections.MutableSequence):
    # http://docs.python.org/2/library/functions.html#list
    # http://hg.python.org/cpython/file/2.7/Lib/_abcoll.py#l517
    # http://redis.io/commands#list

    def __init__(self, values=None, **kwargs):
        super(List, self).__init__(**kwargs)
        if values:
            self.extend(values)

    def __len__(self):
        return self.redis.llen(self.key)

    def __iter__(self):
        values = self.redis.lrange(self.key, 0, -1)
        return (self._unpickle(v) for v in values)

    def __reversed__(self):
        values = self.redis.lrange(self.key, 0, -1)
        return (self._unpickle(v) for v in reversed(values))

    def _recalc_slice(self, start, stop):
        """Slicing in Redis takes also the item at 'stop' index, so there is
        some recalculation to be done.
        """
        start = start or 0
        if stop is None:
            stop = -1
        else:
            stop = stop - 1 if stop != 0 else stop
        return start, stop

    def _calc_overflow(self, size, index):
        return (index >= size) if (index >= 0) else (abs(index) > size)

    def __getitem__(self, index):
        if isinstance(index, slice):
            start, stop = self._recalc_slice(index.start, index.stop)
            values = self.redis.lrange(self.key, start, stop)
            if index.step:
                # step implemented by pure Python slicing
                values = values[::index.step]
            return self._create_instance(map(self._unpickle, values))

        pipe = self.redis.pipeline()
        pipe.llen(self.key)
        pipe.lindex(self.key, index)
        size, value = pipe.execute()

        if self._calc_overflow(size, index):
            raise IndexError(index)
        return self._unpickle(value)

    def __setitem__(self, index, value):
        if isinstance(index, slice):
            if value:
                # assigning anything else than empty lists not supported
                raise NotImplementedError(self.not_impl_msg)
            self.__delitem__(index)
        else:
            def set_trans(pipe):
                size = pipe.llen(self.key)
                if self._calc_overflow(size, index):
                    raise IndexError(index)
                pipe.multi()
                pipe.lset(self.key, index, self._pickle(value))

            self.redis.transaction(set_trans, self.key)

    def __delitem__(self, index):
        begin = 0
        end = -1

        if isinstance(index, slice):
            if index.step:
                # stepping not supported
                raise NotImplementedError(self.not_impl_msg)

            start, stop = self._recalc_slice(index.start, index.stop)

            if start == begin and stop == end:
                # trim from beginning to end
                self.clear()
                return

            pipe = self.redis.pipeline()
            if start != begin and stop == end:
                # right trim
                pipe.ltrim(self.key, begin, start - 1)
            elif start == begin and stop != end:
                # left trim
                pipe.ltrim(self.key, stop + 1, end)
            else:
                # only trimming is supported
                raise NotImplementedError(self.not_impl_msg)
            pipe.execute()
        else:
            if index == begin:
                self.redis.lpop(self.key)
            elif index == end:
                self.redis.rpop(self.key)
            else:
                raise NotImplementedError(self.not_impl_msg)

    def remove(self, value):
        self.redis.lrem(self.key, 1, self._pickle(value))

    def index(self, value, start=None, stop=None):
        start, stop = self._recalc_slice(start, stop)
        values = self.redis.lrange(self.key, start, stop)

        for i, v in enumerate(self._unpickle(v) for v in values):
            if v == value:
                return i + start
        raise ValueError(value)

    def insert(self, index, value):
        def insert_trans(pipe):
            size = pipe.llen(self.key)
            pipe.multi()

            pickled_value = self._pickle(value)
            if index < 0 and abs(index) > size:
                pipe.lpush(self.key, pickled_value)
            elif index >= size:
                pipe.rpush(self.key, pickled_value)
            else:
                pipe.lset(self.key, index, pickled_value)

        self.redis.transaction(insert_trans, self.key)

    def extend(self, values):
        values = map(self._pickle, values)
        self.redis.rpush(self.key, *values)

    def pop(self, index=-1):
        if index == 0:
            value = self.redis.lpop(self.key)
        elif index == -1:
            value = self.redis.rpop(self.key)
        else:
            raise NotImplementedError(self.not_impl_msg)
        return self._unpickle(value)

    def __add__(self, values):
        other = self._create_instance(values=self)
        other.extend(values)
        return other

    def __mul__(self, n):
        if not isinstance(n, int):
            raise TypeError('Cannot multiply sequence by non-int.')
        other = self._create_instance()
        for _ in xrange(0, n):
            other.extend(self)
        return other

    def __rmul__(self, n):
        return self.__mul__(n)


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
