# -*- coding: utf-8 -*-
"""
dicts
~~~~~

Collections based on dict interface.
"""
from __future__ import division, print_function, unicode_literals

import collections
import operator

import six

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

    if six.PY2:
        _pickle_key = RedisCollection._pickle_2
        _unpickle_key = RedisCollection._unpickle_2
    else:
        _pickle_key = RedisCollection._pickle_3

    _pickle_value = RedisCollection._pickle_3

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
        :param writeback: If ``True`` keep a local cache of changes for storing
                          modifications to mutable values. Changes will be
                          written to Redis after calling the ``sync`` method.
        :type key: bool

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
        data = args[0] if args else kwargs.pop('data', None)
        writeback = kwargs.pop('writeback', False)
        super(Dict, self).__init__(**kwargs)

        self.writeback = writeback
        self.cache = {}

        if data:
            self.update(data)

    def __len__(self, pipe=None):
        """Return the number of items in the dictionary."""
        pipe = pipe or self.redis
        return pipe.hlen(self.key)

    def __iter__(self, pipe=None):
        """Return an iterator over the keys of the dictionary."""
        pipe = pipe or self.redis
        for k, v in six.iteritems(self._data(pipe)):
            yield k

    def __contains__(self, key):
        """Return ``True`` if *key* is present, else ``False``."""
        pickled_key = self._pickle_key(key)
        return bool(self.redis.hexists(self.key, pickled_key))

    def getmany(self, *keys):
        """Return the value for *keys*. If particular key is not in the
        dictionary, return :obj:`None`.
        """
        pickled_keys = (self._pickle_key(k) for k in keys)
        pickled_values = self.redis.hmget(self.key, *pickled_keys)

        ret = []
        for k, v in six.moves.zip(keys, pickled_values):
            value = self.cache.get(k, self._unpickle(v))
            ret.append(value)

        return ret

    def __getitem__(self, key):
        """Return the item of dictionary with key *key*. Raises a
        :exc:`KeyError` if key is not in the map.

        If a subclass of :class:`Dict` defines a method :func:`__missing__`, if
        the key *key* is not present, the ``d[key]`` operation calls that
        method with the key *key* as argument. The ``d[key]`` operation
        then returns or raises whatever is returned or raised by
        the ``__missing__(key)`` call if the key is not present.
        """
        try:
            value = self.cache[key]
        except KeyError:
            pickled_key = self._pickle_key(key)
            pickled_value = self.redis.hget(self.key, pickled_key)
            if pickled_value is None:
                if hasattr(self, '__missing__'):
                    return self.__missing__(key)
                raise KeyError(key)

            value = self._unpickle(pickled_value)

        if self.writeback:
            self.cache[key] = value
        return value

    def __setitem__(self, key, value):
        """Set ``d[key]`` to *value*."""
        pickled_key = self._pickle_key(key)
        pickled_value = self._pickle_value(value)
        self.redis.hset(self.key, pickled_key, pickled_value)

        if self.writeback:
            self.cache[key] = value

    def __delitem__(self, key):
        """Remove ``d[key]`` from dictionary.
        Raises a :func:`KeyError` if *key* is not in the map.
        """
        pickled_key = self._pickle_key(key)
        deleted_count = self.redis.hdel(self.key, pickled_key)
        if not deleted_count:
            raise KeyError(key)

        self.cache.pop(key, None)

    def _data(self, pipe=None):
        """
        Returns a Python dictionary with the same values as this object
        (without checking the local cache).
        """
        pipe = pipe or self.redis
        items = six.iteritems(pipe.hgetall(self.key))

        return {self._unpickle(k): self._unpickle(v) for k, v in items}

    def items(self):
        """Return a copy of the dictionary's list of ``(key, value)`` pairs."""
        return list(self.iteritems())

    def iteritems(self, pipe=None):
        """Return an iterator over the dictionary's ``(key, value)`` pairs."""
        pipe = pipe or self.redis
        for k, v in six.iteritems(self._data(pipe)):
            yield k, self.cache.get(k, v)

    def keys(self):
        """Return a copy of the dictionary's list of keys."""
        return list(self.__iter__())

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
        return [v for k, v in self.iteritems()]

    def itervalues(self):
        """Return an iterator over the dictionary's values."""
        return (v for k, v in self.iteritems())

    def pop(self, key, default=__marker):
        """If *key* is in the dictionary, remove it and return its value,
        else return *default*. If *default* is not given and *key* is not
        in the dictionary, a :exc:`KeyError` is raised.
        """
        pickled_key = self._pickle_key(key)

        if key in self.cache:
            self.redis.hdel(self.key, pickled_key)
            return self.cache.pop(key)

        def pop_trans(pipe):
            pickled_value = pipe.hget(self.key, pickled_key)
            if pickled_value is None:
                if default is self.__marker:
                    raise KeyError(key)
                return default

            pipe.hdel(self.key, pickled_key)
            return self._unpickle(pickled_value)

        value = self._transaction(pop_trans)
        self.cache.pop(key, None)

        return value

    def popitem(self):
        """Remove and return an arbitrary ``(key, value)`` pair from
        the dictionary.

        :func:`popitem` is useful to destructively iterate over
        a dictionary, as often used in set algorithms. If
        the dictionary is empty, calling :func:`popitem` raises
        a :exc:`KeyError`.
        """
        def popitem_trans(pipe):
            try:
                pickled_key = pipe.hkeys(self.key)[0]
            except IndexError:
                raise KeyError

            # pop its value
            pipe.multi()
            pipe.hget(self.key, pickled_key)
            pipe.hdel(self.key, pickled_key)
            pickled_value, __ = pipe.execute()

            return self._unpickle(pickled_key), self._unpickle(pickled_value)

        key, value = self._transaction(popitem_trans)

        return key, self.cache.pop(key, value)

    def setdefault(self, key, default=None):
        """If *key* is in the dictionary, return its value.
        If not, insert *key* with a value of *default* and
        return *default*. *default* defaults to :obj:`None`.
        """
        if key in self.cache:
            return self.cache[key]

        def setdefault_trans(pipe):
            pickled_key = self._pickle_key(key)

            pipe.multi()
            pipe.hsetnx(self.key, pickled_key, self._pickle_value(default))
            pipe.hget(self.key, pickled_key)

            __, pickled_value = pipe.execute()

            return self._unpickle(pickled_value)

        value = self._transaction(setdefault_trans)

        if self.writeback:
            self.cache[key] = value
        return value

    def _update_helper(self, other, use_redis=False):
        def _update_helper_trans(pipe):
            data = {}

            if isinstance(other, Dict):
                data.update(other.iteritems(pipe))
            elif isinstance(other, RedisCollection):
                data.update(other.__iter__(pipe))
            else:
                data.update(other)

            pickled_data = {}
            for k, v in six.iteritems(data):
                pickled_data[self._pickle_key(k)] = self._pickle_value(v)

            if pickled_data:
                pipe.hmset(self.key, pickled_data)

            if self.writeback:
                self.cache.update(data)

        if use_redis:
            self._transaction(_update_helper_trans, other.key)
        else:
            self._transaction(_update_helper_trans)

    def update(self, other=None, **kwargs):
        """Update the dictionary with the key/value pairs from *other*,
        overwriting existing keys. Return :obj:`None`.

        :func:`update` accepts either another dictionary object or
        an iterable of key/value pairs (as tuples or other iterables
        of length two). If keyword arguments are specified, the
        dictionary is then updated with those key/value pairs:
        ``d.update(red=1, blue=2)``.
        """
        if other is not None:
            if self._same_redis(other, RedisCollection):
                self._update_helper(other, use_redis=True)
            elif hasattr(other, 'keys'):
                self._update_helper(other)
            else:
                self._update_helper({k: v for k, v in other})

        if kwargs:
            self._update_helper(kwargs)

    def copy(self, key=None):
        other = self.__class__(redis=self.redis, key=key)
        other.update(self)

        return other

    def clear(self, pipe=None):
        self._clear(pipe)

        if self.writeback:
            self.cache.clear()

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

    def _repr_data(self):
        items = ('{}: {}'.format(repr(k), repr(v)) for k, v in self.items())
        return '{{{}}}'.format(', '.join(items))

    def sync(self):
        self.writeback = False
        self._update_helper(self.cache)
        self.cache = {}
        self.writeback = True


class Counter(Dict):
    """Mutable **mapping** collection aiming to have the same API as
    :class:`collections.Counter`. See `Counter
    <http://docs.python.org/2/library/collections.html#collections.Counter>`_
    for further details. The Redis implementation is based on the
    `hash <http://redis.io/commands#hash>`_ type.

    .. warning::
        Not available in Python 2.6.

    .. warning::
        Note that this :class:`Counter` does not implement
        methods :func:`viewitems`, :func:`viewkeys`, and :func:`viewvalues`,
        which are available in Python 2.7's version.
    """

    def __init__(self, *args, **kwargs):
        """Breakes the original :class:`Counter` API, because there is no
        support for keyword syntax. The only single way to create
        :class:`Counter` object is to pass iterable or mapping as the first
        argument. Iterable is expected to be a sequence of elements,
        not a sequence of ``(key, value)`` pairs.

        :param data: Initial data.
        :type data: iterable or mapping
        :param redis: Redis client instance. If not provided, default Redis
                      connection is used.
        :type redis: :class:`redis.StrictRedis`
        :param key: Redis key of the collection. Collections with the same key
                    point to the same data. If not provided, default random
                    string is generated.
        :type key: str

        .. note::
            :func:`uuid.uuid4` is used for default key generation.
            If you are not satisfied with its `collision
            probability <http://stackoverflow.com/a/786541/325365>`_,
            make your own implementation by subclassing and overriding
            internal method :func:`_create_key`.

        .. warning::
            As mentioned, :class:`Counter` does not support following
            initialization syntax: ``c = Counter(a=1, b=2)``
        """
        super(Counter, self).__init__(*args, **kwargs)

    def __missing__(self, key):
        return 0

    def most_common(self, n=None):
        """Return a list of the *n* most common elements and their counts
        from the most common to the least. If *n* is not specified,
        :func:`most_common` returns *all* elements in the counter.
        Elements with equal counts are ordered arbitrarily.
        """
        return collections.Counter(self).most_common(n)

    def elements(self, n=None):
        """Return an iterator over elements repeating each as many times as
        its count. Elements are returned in arbitrary order. If an element's
        count is less than one, :func:`elements` will ignore it.
        """
        return collections.Counter(self).elements()

    @classmethod
    def fromkeys(cls, iterable, v=None):
        raise NotImplementedError(
            'Counter.fromkeys() is undefined.  Use Counter(iterable) instead.'
        )

    def _update_helper(self, other, op, use_redis=False):
        def _update_helper_trans(pipe):
            data = {}

            if isinstance(other, Dict):
                data.update(other.iteritems(pipe))
            elif isinstance(other, RedisCollection):
                data.update(other.__iter__(pipe))
            else:
                data.update(other)

            pickled_data = {}
            for k, v in six.iteritems(data):
                pickled_key = self._pickle_key(k)
                pickled_value = self._pickle_value(op(self.get(k, 0), v))
                pickled_data[pickled_key] = pickled_value

            if pickled_data:
                pipe.hmset(self.key, pickled_data)

            if self.writeback:
                self.cache.update(data)

        if use_redis:
            self._transaction(_update_helper_trans, other.key)
        else:
            self._transaction(_update_helper_trans)

    def update(self, other=None, **kwargs):
        """Elements are counted from an *iterable* or added-in from another
        *mapping* (or counter). Like :func:`dict.update` but adds counts
        instead of replacing them. Also, the *iterable* is expected to be
        a sequence of elements, not a sequence of ``(key, value)`` pairs.
        """
        if other is not None:
            if self._same_redis(other, RedisCollection):
                self._update_helper(other, operator.add, use_redis=True)
            elif hasattr(other, 'keys'):
                self._update_helper(other, operator.add)
            else:
                self._update_helper(collections.Counter(other), operator.add)

        if kwargs:
            self._update_helper(kwargs, operator.add)

    def subtract(self, other=None, **kwargs):
        """Elements are subtracted from an *iterable* or from another
        *mapping* (or counter). Like :func:`dict.update` but subtracts
        counts instead of replacing them.
        """
        if other is not None:
            if self._same_redis(other, RedisCollection):
                self._update_helper(other, operator.sub, use_redis=True)
            elif hasattr(other, 'keys'):
                self._update_helper(other, operator.sub)
            else:
                self._update_helper(collections.Counter(other), operator.sub)

        if kwargs:
            self._update_helper(kwargs, operator.sub)

    def __delitem__(self, key):
        """Like :func:`dict.__delitem__`, but does not raise KeyError for
        missing values.
        """
        try:
            super(Counter, self).__delitem__(key)
        except KeyError:
            pass

    def _op_helper(self, other, op, swap_args=False, inplace=False):
        def op_trans(pipe):
            # Get a collections.Counter copy of `self`
            self_counter = collections.Counter(
                {k: v for k, v in self.iteritems(pipe=pipe)}
            )

            # If `other` is also Redis-backed we'll want to pull its values
            # with the same transaction-provided pipeline as for `self`.
            if isinstance(other, Dict):
                other_counter = collections.Counter(
                    {k: v for k, v in other.iteritems(pipe=pipe)}
                )
            elif isinstance(other, RedisCollection):
                other_counter = collections.Counter(other.__iter__(pipe))
            else:
                other_counter = other

            # Unary case
            if other is None:
                result = op(self_counter)
            # Reversed case
            elif swap_args:
                result = op(other_counter, self_counter)
            # Normal case
            else:
                result = op(self_counter, other_counter)

            # If we're not updating `self`, we're finished
            if not inplace:
                return result

            # Otherwise we need to update `self` in this transaction
            pickled_data = {}
            for key, value in six.iteritems(result):
                pickled_key = self._pickle_key(key)
                pickled_value = self._pickle_value(value)
                pickled_data[pickled_key] = pickled_value

            pipe.multi()
            pipe.delete(self.key)
            if pickled_data:
                pipe.hmset(self.key, pickled_data)

        if other is None:
            result = self._transaction(op_trans, None)
        elif self._same_redis(other, RedisCollection):
            result = self._transaction(op_trans, other.key)
        elif isinstance(other, collections.Counter):
            result = self._transaction(op_trans)
        else:
            raise TypeError('Unsupported type {}'.format(type(other)))

        return self if inplace else result

    def __add__(self, other):
        return self._op_helper(other, operator.add)

    def __radd__(self, other):
        return self._op_helper(other, operator.add, swap_args=True)

    def __sub__(self, other):
        return self._op_helper(other, operator.sub)

    def __rsub__(self, other):
        return self._op_helper(other, operator.sub, swap_args=True)

    def __or__(self, other):
        return self._op_helper(other, operator.or_)

    def __ror__(self, other):
        return self._op_helper(other, operator.or_, swap_args=True)

    def __and__(self, other):
        return self._op_helper(other, operator.and_)

    def __rand__(self, other):
        return self._op_helper(other, operator.and_, swap_args=True)

    def __iadd__(self, other):
        return self._op_helper(other, operator.add, inplace=True)

    def __isub__(self, other):
        return self._op_helper(other, operator.sub, inplace=True)

    def __ior__(self, other):
        return self._op_helper(other, operator.ior, inplace=True)

    def __iand__(self, other):
        return self._op_helper(other, operator.iand, inplace=True)

    if not six.PY2:
        def __pos__(self):
            return self._op_helper(None, operator.pos)

        def __neg__(self):
            return self._op_helper(None, operator.neg)


class DefaultDict(Dict):
    """Mutable **mapping** collection aiming to have the same API as
    :class:`collections.defaultdict`. See
    `defaultdict  <https://docs.python.org/2/library/collections.html`_ for
    further details. The Redis implementation is based on the
    `hash <http://redis.io/commands#hash>`_ type.

    .. warning::
        Note that this :class:`DefaultDict` does not implement
        methods :func:`viewitems`, :func:`viewkeys`, and :func:`viewvalues`,
        which are available in Python 2.7's version.
    """

    def __init__(self, *args, **kwargs):
        """Breakes the original :class:`defaultdict` API, because there is no
        support for keyword syntax. The only single way to create
        :class:`defaultdict` object is to pass an iterable or mapping as the
        second argument.

        :param default_factory: Used to provide default values for missing
                                keys.
        :type default_factory: callable or None
        :param data: Initial data.
        :type data: iterable or mapping
        :param redis: Redis client instance. If not provided, default Redis
                      connection is used.
        :type redis: :class:`redis.StrictRedis`
        :param key: Redis key of the collection. Collections with the same key
                    point to the same data. If not provided, default random
                    string is generated.
        :type key: str

        .. note::
            :func:`uuid.uuid4` is used for default key generation.
            If you are not satisfied with its `collision
            probability <http://stackoverflow.com/a/786541/325365>`_,
            make your own implementation by subclassing and overriding
            internal method :func:`_create_key`.

        .. warning::
            As mentioned, :class:`DefaultDict` does not support following
            initialization syntax: ``d = DefaultDict(None, a=1, b=2)``
        """
        kwargs.setdefault('writeback', True)
        if args:
            default_factory = args[0]
            args = args[1:]
        else:
            default_factory = None

        super(DefaultDict, self).__init__(*args, **kwargs)

        if default_factory is None:
            pass
        elif not callable(default_factory):
            raise TypeError('first argument must be callable or None')
        self.default_factory = default_factory

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)

        value = self.default_factory()
        self[key] = value
        return value

    def copy(self, key=None):
        other = self.__class__(self.default_factory, redis=self.redis, key=key)
        other.update(self)

        return other
