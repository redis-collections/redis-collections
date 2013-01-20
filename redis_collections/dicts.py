# -*- coding: utf-8 -*-
"""
dicts
~~~~~

Collections based on dict interface.
"""


import collections

from .base import RedisCollection


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
            return '<missing value>'  # for purposes of generated documentation
    __marker = __missing_value()

    def __init__(self, *args, **kwargs):
        """Breakes the original :class:`dict` API, because there is no support
        for keyword syntax. The only single way to create :class:`Dict`
        object is to pass iterable or mapping as the first argument.

        :param data: Initial data.
        :type data: iterable or mapping
        :param redis: Redis client instance. If not provided, default Redis
                      connection is used.
        :type redis: :class:`redis.StrictRedis`
        :param key: Redis key of the collection. Collections with the same key
                    point to the same data. If not provided, default random
                    string is generated.
        :type key: str
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
        :param prefix: Key prefix to use when working with Redis. Defaults
                       to empty string.
        :type prefix: str

        .. note::
            :func:`uuid.uuid4` is used for default key generation.
            If you are not satisfied with its `collision
            probability <http://stackoverflow.com/a/786541/325365>`_,
            make your own implementation by subclassing and overriding
            internal method :func:`_create_key`.

        .. warning::
            As mentioned, :class:`Dict` does not support following
            initialization syntax: ``d = Dict(a=1, b=2)``
        """
        super(Dict, self).__init__(*args, **kwargs)

    def __len__(self):
        """Return the number of items in the dictionary."""
        return self.redis.hlen(self.key)

    def __iter__(self):
        """Return an iterator over the keys of the dictionary."""
        return iter(self.redis.hkeys(self.key))

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

    def getmany(self, *keys):
        """Return the value for *keys*. If particular key is not in the
        dictionary, return :obj:`None`.
        """
        values = self.redis.hmget(self.key, *keys)
        return map(self._unpickle, values)

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
        with self.redis.pipeline() as pipe:
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
        with self.redis.pipeline() as pipe:
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

    def _data(self, pipe=None):
        redis = pipe or self.redis
        result = redis.hgetall(self.key).items()
        return [(k, self._unpickle(v)) for (k, v) in result]

    def items(self):
        """Return a copy of the dictionary's list of ``(key, value)`` pairs."""
        return self._data()

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
        return self.__iter__()

    def iterkeys(self):
        """Return an iterator over the dictionary's keys."""
        return self.__iter__()

    def values(self):
        """Return a copy of the dictionary's list of values."""
        result = self.redis.hvals(self.key)
        return [self._unpickle(v) for v in result]

    def itervalues(self):
        """Return an iterator over the dictionary's values."""
        result = iter(self.redis.hvals(self.key))
        return (self._unpickle(v) for v in result)

    def pop(self, key, default=__marker):
        """If *key* is in the dictionary, remove it and return its value,
        else return *default*. If *default* is not given and *key* is not
        in the dictionary, a :exc:`KeyError` is raised.
        """
        with self.redis.pipeline() as pipe:
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
        def popitem_trans(pipe):
            # get an 'arbitrary' key
            try:
                key = pipe.hkeys(self.key)[0]
            except IndexError:
                raise KeyError

            # pop its value
            pipe.multi()
            pipe.hget(self.key, key)
            pipe.hdel(self.key, key)
            value, _ = pipe.execute()

            return key, value

        key, value = self._transaction(popitem_trans)
        return key, self._unpickle(value)

    def setdefault(self, key, default=None):
        """If *key* is in the dictionary, return its value.
        If not, insert *key* with a value of *default* and
        return *default*. *default* defaults to :obj:`None`.
        """
        with self.redis.pipeline() as pipe:
            pipe.hsetnx(self.key, key, self._pickle(default))
            pipe.hget(self.key, key)
            _, value = pipe.execute()
        return self._unpickle(value)

    def _update(self, data, pipe=None):
        super(Dict, self)._update(data, pipe)
        redis = pipe or self.redis

        data = dict(data)
        keys = data.keys()
        values = map(self._pickle, data.values())  # pickling values

        redis.hmset(self.key, dict(zip(keys, values)))

    def update(self, other=None, **kwargs):
        """Update the dictionary with the key/value pairs from *other*,
        overwriting existing keys. Return :obj:`None`.

        :func:`update` accepts either another dictionary object or
        an iterable of key/value pairs (as tuples or other iterables
        of length two). If keyword arguments are specified, the
        dictionary is then updated with those key/value pairs:
        ``d.update(red=1, blue=2)``.
        """
        other = other or {}
        if isinstance(other, RedisCollection):
            # wrap into transaction
            def update_trans(pipe):
                d = other._data(pipe=pipe)  # retrieve
                pipe.multi()
                self._update(d, pipe=pipe)  # store
            self._transaction(update_trans)
        else:
            mapping = {}
            mapping.update(other, **kwargs)
            self._update(mapping)

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

    def _repr_data(self, data):
        return repr(dict(data))
