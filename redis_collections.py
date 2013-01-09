# -*- coding: utf-8 -*-
"""
redis_collections
~~~~~~~~~~~~~~~~~
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
    """Abstract class providing backend functionality for all the other
    Redis collections.
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
        :type redis: :class:`redis.StrictRedis` or :obj:`None`
        :param id: ID of the collection. Collections with the same IDs point
                   to the same data. If not provided, default random ID string
                   is generated. If no non-conflicting ID can be found,
                   :exc:`RuntimeError` is raised.
        :type id: str or :obj:`None`
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
        :type prefix: str or :obj:`None`
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
        :type id: string or :obj:`None`
        :param prefix: Key prefix to use when working with Redis.
        :type prefix: string or :obj:`None`
        :rtype: tuple of strings (key, id)
        """
        def create_key(id):
            name = self.__class__.__name__.lower()
            components = [prefix, '_redis_collections', '_' + name, id]
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
        If :obj:`None` or empty string given, :obj:`None` is returned.

        :param string: String to be unserialized.
        :type string: string
        :rtype: anything serializable or :obj:`None`
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
    """Mutable **mapping** collection aiming to have the same API as the
    standard mapping type, dict. See `dict
    <http://docs.python.org/2/library/stdtypes.html#mapping-types-dict>`_ for
    further details. The Redis implementation is based on the
    `hash <http://redis.io/commands#hash>`_ type.

    .. warning::
        In comparing with original :class:`dict` type, :class:`Dict` does not
        implement methods :func:`viewitems`, :func:`viewkeys`, and
        :func:`viewvalues`.

    .. note::
        Some operations, which are usually not used so often, can be more
        efficient than their "popular" equivalents. For example, :func:`get`
        should be preffered over the classic ``d[key]`` approach.
    """

    class __missing_value(object):
        def __repr__(self):
            return '<missing value>'  # for documentation purposes
    __marker = __missing_value()

    def __init__(self, values=None, **kwargs):
        """Breakes the original :class:`dict` API, because there is no support
        for keyword syntax. The only single way to create :class:`Dict`
        object is to pass iterable or mapping as the first argument.
        Remaining arguments are given to :func:`RedisCollection.__init__`.

        .. warning::
            As mentioned, :class:`Dict` does not support following
            initialization syntax: ``d = Dict(a=1, b=2)``
        """
        super(Dict, self).__init__(**kwargs)
        if values:
            self.update(values)

    def __len__(self):
        """Return the number of items in the dictionary."""
        return self.redis.hlen(self.key)

    def __iter__(self):
        """Return an iterator over the keys of the dictionary."""
        return self.iterkeys()

    def __contains__(self, key):
        """Return ``True`` if ``Dict`` instance has a key
        *key*, else ``False``.
        """
        return self.redis.hexists(self.key, key)

    def get(self, key, default=None):
        """Return the value for *key* if *key* is in the dictionary, else
        *default*. If *default* is not given, it defaults to :obj:`None`,
        so that this method never raises a :exc:`KeyError`.

        .. note::
            Due to implementation on Redis side, this method of retrieving
            items is more efficient than classic approach over using the
            :func:`__getitem__` protocol.
        """
        value = self.redis.hget(self.key, key)
        return self._unpickle(value) or default

    def __getitem__(self, key):
        """Return the item of dictionary with key *key*. Raises a
        :exc:`KeyError` if key is not in the map.

        If a subclass of :class:`Dict` defines a method :func:`__missing__`, if
        the key *key* is not present, the ``d[key]`` operation calls that
        method with the key *key* as argument. The ``d[key]`` operation
        then returns or raises whatever is returned or raised by
        the ``__missing__(key)`` call if the key is not present.

        .. note::
            Due to implementation on Redis side, this method of retrieving
            items is not very efficient. If possible, use :func:`get`.
        """
        pipe = self.redis.pipeline()
        pipe.hexists(self.key, key)
        pipe.hget(self.key, key)
        exists, value = pipe.execute()

        if not exists:
            if hasattr(self, '__missing__'):
                return self.__missing__(key)
            raise KeyError(key)
        return self._unpickle(value)

    def __setitem__(self, key, value):
        """Set ``d[key]`` to *value*."""
        value = self._pickle(value)
        self.redis.hset(self.key, key, value)

    def __delitem__(self, key):
        """Remove ``d[key]`` from dictionary. Raises
        a :func:`KeyError` if *key* is not in the map.

        .. note::
            Due to implementation on Redis side, this method of deleting
            items is not very efficient. If possible, use :func:`discard`.
        """
        pipe = self.redis.pipeline()
        pipe.hexists(self.key, key)
        pipe.hdel(self.key, key)
        exists, _ = pipe.execute()

        if not exists:
            raise KeyError(key)

    def discard(self, key):
        """Remove ``d[key]`` from dictionary if it is present.

        .. note::
            Due to implementation on Redis side, this method of retrieving
            items is more efficient than classic approach over using the
            :func:`__delitem__` protocol.
        """
        self.redis.hdel(self.key, key)

    def items(self):
        """Return a copy of the dictionary's list of ``(key, value)`` pairs."""
        result = self.redis.hgetall(self.key).items()
        return [(k, self._unpickle(v)) for (k, v) in result]

    def iteritems(self):
        """Return an iterator over the dictionary's ``(key, value)`` pairs."""
        result = self.redis.hgetall(self.key).iteritems()
        return ((k, self._unpickle(v)) for (k, v) in result)

    def keys(self):
        """Return a copy of the dictionary's list of keys."""
        return self.redis.hkeys(self.key)

    def iter(self):
        """Return an iterator over the keys of the dictionary.
        This is a shortcut for :func:`iterkeys()`.
        """
        return self.iterkeys()

    def iterkeys(self):
        """Return an iterator over the dictionary's keys."""
        return iter(self.redis.hkeys(self.key))

    def values(self):
        """Return a copy of the dictionary's list of values."""
        result = self.redis.hvals(self.key)
        return [self._unpickle(v) for v in result]

    def itervalues(self):
        """Return an iterator over the dictionary's values."""
        result = iter(self.redis.hvals(self.key))
        return (self._unpickle(v) for v in result)

    def copy(self):
        """Return a copy of the dictionary."""
        return self._create_instance(values=self)

    def pop(self, key, default=__marker):
        """If *key* is in the dictionary, remove it and return its value,
        else return *default*. If *default* is not given and *key* is not
        in the dictionary, a :exc:`KeyError` is raised.
        """
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
        """Remove and return an arbitrary ``(key, value)`` pair from
        the dictionary.

        :func:`popitem` is useful to destructively iterate over
        a dictionary, as often used in set algorithms. If
        the dictionary is empty, calling :func:`popitem` raises
        a :exc:`KeyError`.
        """
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
        """If *key* is in the dictionary, return its value.
        If not, insert *key* with a value of *default* and
        return *default*. *default* defaults to :obj:`None`.
        """
        pipe = self.redis.pipeline()
        pipe.hsetnx(self.key, key, self._pickle(default))
        pipe.hget(self.key, key)
        _, value = pipe.execute()

        return self._unpickle(value)

    def update(self, *args, **kwargs):
        """
        Update the dictionary with the key/value pairs from *other*,
        overwriting existing keys. Return :obj:`None`.

        :func:`update` accepts either another dictionary object or
        an iterable of key/value pairs (as tuples or other iterables
        of length two). If keyword arguments are specified, the
        dictionary is then updated with those key/value pairs:
        ``d.update(red=1, blue=2)``.
        """
        mapping = {}
        mapping.update(*args, **kwargs)

        keys = mapping.keys()
        values = map(self._pickle, mapping.values())  # pickling values

        self.redis.hmset(self.key, dict(zip(keys, values)))

    @classmethod
    def fromkeys(cls, seq, value=None, **kwargs):
        """Create a new dictionary with keys from *seq* and values set to
        *value*.

        .. note::
            :func:`fromkeys` is a class method that returns a new dictionary.
            *value* defaults to :obj:`None`. It is possible to specify
            additional keyword arguments to be passed to :func:`__init__` of
            the new object.
        """
        values = ((item, value) for item in seq)
        return cls(values, **kwargs)


class List(RedisCollection, collections.MutableSequence):
    """Mutable **sequence** collection aiming to have the same API as the
    standard sequence type, :class:`list`. See `list
    <http://docs.python.org/2/library/functions.html#list>`_ for
    further details. The Redis implementation is based on the
    `list <http://redis.io/commands#list>`_ type.

    .. warning::
        In comparing with original :class:`list` type, :class:`List` does not
        implement methods :func:`sort` and :func:`reverse`.

    .. note::
        Some operations, which are usually not used so often, can be more
        efficient than their "popular" equivalents. Some operations are
        shortened in their capabilities and can raise
        :exc:`NotImplementedError` for some inputs (e.g. most of the slicing
        functionality).
    """

    def __init__(self, values=None, **kwargs):
        """Pass iterable as the first argument. Remaining arguments are given
        to :func:`RedisCollection.__init__`.
        """
        super(List, self).__init__(**kwargs)
        if values:
            self.extend(values)

    def __len__(self):
        """Length of the sequence."""
        return self.redis.llen(self.key)

    def __iter__(self):
        """Return an iterator over the sequence."""
        values = self.redis.lrange(self.key, 0, -1)
        return (self._unpickle(v) for v in values)

    def __reversed__(self):
        """Returns iterator for the sequence in reversed order."""
        values = self.redis.lrange(self.key, 0, -1)
        return (self._unpickle(v) for v in reversed(values))

    def _recalc_slice(self, start, stop):
        """Slicing in Redis takes also the item at 'stop' index, so there is
        some recalculation to be done. Method returns tuple ``(start, stop)``
        where both values are recalculated to numbers in Redis terms.

        :param start: Index starting the range (in Python terms).
        :param stop: Index where the range ends (in Python terms).
        """
        start = start or 0
        if stop is None:
            stop = -1
        else:
            stop = stop - 1 if stop != 0 else stop
        return start, stop

    def _calc_overflow(self, size, index):
        """Index overflow detection. Returns :obj:`True` if *index* is out of
        range for the given *size*. Otherwise returns :obj:`False`.

        :param size: Size of the collection.
        :param index: Index to be examined.
        """
        return (index >= size) if (index >= 0) else (abs(index) > size)

    def __getitem__(self, index):
        """Returns item of sequence on *index*.
        Origin of indexes is 0. Accepts also slicing.

        .. note::
            Due to implementation on Redis side, ``l[index]`` is not very
            efficient operation. If possible, use :func:`get`. Slicing without
            steps is efficient. Steps are implemented only on Python side.
        """
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

    def get(self, index, default=None):
        """Return the value for *index* if *index* is not out of range, else
        *default*. If *default* is not given, it defaults to :obj:`None`, so
        that this method never raises a :exc:`IndexError`.

        .. note::
            Due to implementation on Redis side, this method of retrieving
            items is more efficient than classic approach over using the
            :func:`__getitem__` protocol.
        """
        value = self.redis.lindex(self.key, index)
        return self._unpickle(value) or default

    def __setitem__(self, index, value):
        """Item of *index* is replaced by *value*.

        .. warning::
            Slicing is generally not supported. Only empty lists are accepted
            if the operation leads into trimming::

                l[2:] = []
                l[:2] = []
                l[:] = []
        """
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
        """Item of *index* is deleted.

        .. warning::
            Slicing is generally not supported. Only empty lists are accepted
            if the operation leads into trimming::

                del l[2:]
                del l[:2]
                del l[:]
        """
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
        """Remove the first occurence of *value*."""
        self.redis.lrem(self.key, 1, self._pickle(value))

    def index(self, value, start=None, stop=None):
        """Returns index of the first occurence of *value*.

        If *start* or *stop* are provided, returns the smallest
        index such that ``s[index] == value`` and ``start <= index < stop``.
        """
        start, stop = self._recalc_slice(start, stop)
        values = self.redis.lrange(self.key, start, stop)

        for i, v in enumerate(self._unpickle(v) for v in values):
            if v == value:
                return i + start
        raise ValueError(value)

    def insert(self, index, value):
        """Item of *index* is replaced by *value*. If *index* is out of
        range, the *value* is prepended or appended (no error is raised).
        """
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
        """*values* are appended at the end of the list. Any iterable
        is accepted.
        """
        values = map(self._pickle, values)
        self.redis.rpush(self.key, *values)

    def pop(self, index=-1):
        """Item on *index* is removed and returned.

        .. warning::
            Only indexes ``0`` and ``-1`` (default) are supported, otherwise
            :exc:`NotImplementedError` is raised.
        """
        if index == 0:
            value = self.redis.lpop(self.key)
        elif index == -1:
            value = self.redis.rpop(self.key)
        else:
            raise NotImplementedError(self.not_impl_msg)
        return self._unpickle(value)

    def __add__(self, values):
        """Returns concatenation of the list and given iterable.

        New :class:`List` instance is returned. The same arguments given to
        :func:`__init__` of the list are used for creation of the new
        instance.
        """
        other = self._create_instance(values=self)
        other.extend(values)
        return other

    def __mul__(self, n):
        """Returns *n* copies of the list, concatenated.

        .. note::
            New :class:`List` instance is returned. The same arguments given to
            :func:`__init__` of this list are used for creation of the new
            instance.
        """
        if not isinstance(n, int):
            raise TypeError('Cannot multiply sequence by non-int.')
        return self._create_instance(list(self) * n)

    def __rmul__(self, n):
        """Returns *n* copies of the list, concatenated.

        .. note::
            New :class:`List` instance is returned. The same arguments given to
            :func:`__init__` of this list are used for creation of the new
            instance.
        """
        return self.__mul__(n)


class Set(RedisCollection, collections.MutableSet):
    """Mutable **set** collection aiming to have the same API as the standard
    set type. See `set
    <http://docs.python.org/2/library/stdtypes.html#set>`_ for
    further details. The Redis implementation is based on the
    `list <http://redis.io/commands#set>`_ type.
    """

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
    """Mutable **sorted set** collection aiming to have the same API as the
    standard set type. See `set
    <http://docs.python.org/2/library/stdtypes.html#set>`_ for
    further details. The Redis implementation is based on the
    `list <http://redis.io/commands#sorted_set>`_ type.
    """

    # http://code.activestate.com/recipes/576694/

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


class Deque(List):
    pass


class Counter(Dict):
    pass


class DefaultDict(Dict):
    pass
