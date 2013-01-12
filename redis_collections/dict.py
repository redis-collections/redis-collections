# -*- coding: utf-8 -*-
"""
dict
~~~~
"""


import redis
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

        .. warning::
            **Operation is not atomic.**
        """
        super(Dict, self).__init__(**kwargs)

        if values is not None:
            self.clear()
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
        """Return a copy of the dictionary.

        .. warning::
            **Operation is not atomic.**
        """
        return self._create_instance(self)

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
