# -*- coding: utf-8 -*-
"""
dicts
~~~~~

Collections based on dict interface.
"""
from __future__ import division, print_function, unicode_literals

import collections

import six

from .base import RedisCollection, same_types


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
        super(Dict, self).__init__(*args, **kwargs)

        if data:
            self.update(data)

    def _get_hash_dict(self, key, redis):
        key_hash = hash(key)
        D = redis.hget(self.key, key_hash)
        D = {} if D is None else self._unpickle(D)

        return key_hash, D

    def __len__(self):
        """Return the number of items in the dictionary.

        .. note::
            Due to implementation on Redis side, this method is inefficient.
            The time taken is varies with the number of keys in stored.
        """
        ret = 0
        for D in six.itervalues(self.redis.hgetall(self.key)):
            ret += len(self._unpickle(D))

        return ret

    def __iter__(self):
        """Return an iterator over the keys of the dictionary."""
        return self.iterkeys()

    def __contains__(self, key):
        """Return ``True`` if *key* is present, else ``False``."""
        key_hash, D = self._get_hash_dict(key, self.redis)

        return key in D

    def getmany(self, *keys):
        """Return the value for *keys*. If particular key is not in the
        dictionary, return :obj:`None`.
        """
        ret = []
        for D in self.redis.hmget(self.key, *(hash(k) for k in keys)):
            if D is None:
                ret.append(None)
            else:
                for v in six.itervalues(self._unpickle(D)):
                    ret.append(v)

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
        key_hash, D = self._get_hash_dict(key, self.redis)

        try:
            value = D[key]
        except KeyError:
            if hasattr(self, '__missing__'):
                return self.__missing__(key)
            else:
                raise

        return value

    def __setitem__(self, key, value):
        """Set ``d[key]`` to *value*."""
        key_hash, D = self._get_hash_dict(key, self.redis)
        D[key] = value

        self.redis.hset(self.key, key_hash, self._pickle(D))

    def __delitem__(self, key):
        """Remove ``d[key]`` from dictionary.
        Raises a :func:`KeyError` if *key* is not in the map.
        """
        key_hash, D = self._get_hash_dict(key, self.redis)
        del D[key]

        if D:
            self.redis.hset(self.key, key_hash, self._pickle(D))
        else:
            self.redis.hdel(self.key, key_hash)

    def _data(self, pipe=None):
        redis = self.redis if pipe is None else pipe

        ret = []
        for D in six.itervalues(redis.hgetall(self.key)):
            for k, v in six.iteritems(self._unpickle(D)):
                ret.append((k, v))

        return ret

    def items(self):
        """Return a copy of the dictionary's list of ``(key, value)`` pairs."""
        return list(self.iteritems())

    def iteritems(self):
        """Return an iterator over the dictionary's ``(key, value)`` pairs."""
        for D in six.itervalues(self.redis.hgetall(self.key)):
            for k, v in six.iteritems(self._unpickle(D)):
                yield k, v

    def keys(self):
        """Return a copy of the dictionary's list of keys."""
        return list(self.iterkeys())

    def iter(self):
        """Return an iterator over the keys of the dictionary.
        This is a shortcut for :func:`iterkeys()`.
        """
        return self.__iter__()

    def iterkeys(self):
        """Return an iterator over the dictionary's keys."""
        for D in six.itervalues(self.redis.hgetall(self.key)):
            for k in six.iterkeys(self._unpickle(D)):
                yield k

    def values(self):
        """Return a copy of the dictionary's list of values."""
        return list(self.itervalues())

    def itervalues(self):
        """Return an iterator over the dictionary's values."""
        for D in six.itervalues(self.redis.hgetall(self.key)):
            for k, v in six.iteritems(self._unpickle(D)):
                yield v

    def pop(self, key, default=__marker):
        """If *key* is in the dictionary, remove it and return its value,
        else return *default*. If *default* is not given and *key* is not
        in the dictionary, a :exc:`KeyError` is raised.
        """
        def pop_trans(pipe):
            key_hash, D = self._get_hash_dict(key, pipe)
            value = D.pop(key, default)

            pipe.multi()
            if D:
                pipe.hset(self.key, key_hash, self.pickle(D))
            else:
                pipe.hdel(self.key, key_hash)

            return value

        value = self._transaction(pop_trans)
        if value is self.__marker:
            raise KeyError(key)

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
            entries = pipe.hgetall(self.key)
            if not entries:
                raise KeyError

            key_hash, D = entries.popitem()
            D = self._unpickle(D)
            item = D.popitem()

            pipe.multi()
            if D:
                pipe.hset(self.key, key_hash, self._pickle(D))
            else:
                pipe.hdel(self.key, key_hash)

            return item

        key, value = self._transaction(popitem_trans)
        return key, value

    def setdefault(self, key, default=None):
        """If *key* is in the dictionary, return its value.
        If not, insert *key* with a value of *default* and
        return *default*. *default* defaults to :obj:`None`.
        """
        def setdefault_trans(pipe):
            key_hash, D = self._get_hash_dict(key, pipe)
            value = D.setdefault(key, default)
            if value == default:
                pipe.hset(self.key, key_hash, self._pickle(D))

            return value

        value = self._transaction(setdefault_trans)
        return value

    def _update_helper(self, other, use_redis=False):
        def _update_helper_trans(pipe):
            data = {}

            if use_redis:
                for D in six.itervalues(pipe.hgetall(other.key)):
                    data.update(self._unpickle(D))
            else:
                data.update(other)

            D_load = {}
            for key, value in six.iteritems(data):
                key_hash = hash(key)
                D_load.setdefault(key_hash, {})
                D_load[key_hash][key] = value

            for key_hash, D in six.iteritems(D_load):
                pipe.hset(self.key, key_hash, self._pickle(D))

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
            if isinstance(other, RedisCollection):
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


class Counter(Dict):
    """Mutable **mapping** collection aiming to have the same API as
    :class:`collections.Counter`. See `Counter
    <http://docs.python.org/2/library/collections.html#collections.Counter>`_
    for further details. The Redis implementation is based on the
    `hash <http://redis.io/commands#hash>`_ type.

    .. warning::
        Not available in Python 2.6.

    .. warning::
        In comparing with original :class:`collections.Counter` type
        **supports only integers**. :class:`Counter` also  does not implement
        methods :func:`viewitems`, :func:`viewkeys`, and :func:`viewvalues`.

    .. note::
        Unlike :class:`Dict`, :class:`Counter` has the same efficiency
        of ``c[key]`` and :func:`get` operations.
    """

    _same_types = (collections.Counter,)

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

    def _pickle(self, data):
        return str(int(data)).encode('ascii')

    def _unpickle(self, string):
        if string is None:
            return None
        return int(string)

    def _obj_to_data(self, obj):
        is_redis = isinstance(obj, RedisCollection)
        is_mapping = isinstance(obj, collections.Mapping)

        data = obj._data() if is_redis else obj
        return dict(data) if is_mapping else iter(data)

    def getmany(self, *keys):
        values = super(Counter, self).getmany(*keys)
        return [(v or 0) for v in values]

    def __getitem__(self, key):
        """Return the item of dictionary with key *key*. Returns zero if key
        is not in the map.

        .. note::
            Unlike :class:`Dict`, :class:`Counter` has the same efficiency
            of ``c[key]`` and :func:`get` operations.
        """
        return self.get(key, 0)

    def elements(self):
        """Return an iterator over elements repeating each as many times as
        its count. Elements are returned in arbitrary order. If an element's
        count is less than one, :func:`elements` will ignore it.
        """
        for element, count in self._data():
            if count:
                for _ in six.moves.xrange(0, count):
                    yield element

    def _update(self, data, pipe=None):
        super(Dict, self)._update(data, pipe)  # Dict intentionally
        redis = pipe if pipe is not None else self.redis

        data = collections.Counter(data)

        redis.hmset(
            self.key, {k: self._pickle(v) for k, v in six.iteritems(data)}
        )

    def inc(self, key, n=1):
        """Value of *key* will be increased by *n*. *n* defaults to 1.
        If *n* is negative integer, value of *key* will be decreased.
        Value after the increment (decrement) operation is returned.

        .. note::
            Whole operation is ignored if *n* is zero.

        :rtype: integer
        """
        if n:
            return self.redis.hincrby(self.key, key, self._pickle(n))
        return 0

    def most_common(self, n=None):
        """Return a list of the *n* most common elements and their counts
        from the most common to the least. If *n* is not specified,
        :func:`most_common` returns *all* elements in the counter.
        Elements with equal counts are ordered arbitrarily.
        """
        data = self._obj_to_data(self)
        counter = collections.Counter(data)
        return counter.most_common(n)

    def _operation(self, other, fn, update=False):
        """Update operation helper.

        :param other: Other operand.
        :type other: iterable or mapping
        :param fn: Closure, takes counter object as the first argument
                   and data from *other* as second.
        :type fn: function *fn(counter, other)*
        :param update: Whether the operation is update.
        :type update: boolean
        """
        key = self.key if update else None

        def op_trans(pipe):
            d1 = self._obj_to_data(self)
            d2 = self._obj_to_data(other)

            c1 = collections.Counter(d1)
            result = fn(c1, d2)

            if update:
                result = c1

            pipe.multi()
            return self._create_new(result, key=key, pipe=pipe)
        return self._transaction(op_trans, key)

    def subtract(self, other):
        """Elements are subtracted from an *iterable* or from another
        *mapping* (or counter). Like :func:`dict.update` but subtracts
        counts instead of replacing them.
        """
        def subtract_op(c1, d2):
            c1.subtract(d2)
        self._operation(other, subtract_op, update=True)

    def update(self, other):
        """Elements are counted from an *iterable* or added-in from another
        *mapping* (or counter). Like :func:`dict.update` but adds counts
        instead of replacing them. Also, the *iterable* is expected to be
        a sequence of elements, not a sequence of ``(key, value)`` pairs.
        """
        def update_op(c1, d2):
            c1.update(d2)
        self._operation(other, update_op, update=True)

    @same_types
    def __add__(self, other):
        def add_op(c1, d2):
            c2 = collections.Counter(d2)
            return c1 + c2
        return self._operation(other, add_op)

    def __radd__(self, other):
        return self.__add__(other)

    @same_types
    def __iadd__(self, other):
        def iadd_op(c1, d2):
            c2 = collections.Counter(d2)
            c1 += c2
        return self._operation(other, iadd_op, update=True)

    @same_types
    def __and__(self, other):
        def and_op(c1, d2):
            c2 = collections.Counter(d2)
            return c1 & c2
        return self._operation(other, and_op)

    def __rand__(self, other):
        return self.__and__(other)

    @same_types
    def __iand__(self, other):
        def iand_op(c1, d2):
            c2 = collections.Counter(d2)
            c1 &= c2
        return self._operation(other, iand_op, update=True)

    @same_types
    def __sub__(self, other):
        def sub_op(c1, d2):
            c2 = collections.Counter(d2)
            return c1 - c2
        return self._operation(other, sub_op)

    @same_types
    def __rsub__(self, other):
        def rsub_op(c1, d2):
            c2 = collections.Counter(d2)
            return c2 - c1
        return self._operation(other, rsub_op)

    @same_types
    def __isub__(self, other):
        def isub_op(c1, d2):
            c2 = collections.Counter(d2)
            c1 -= c2
        return self._operation(other, isub_op, update=True)

    @same_types
    def __or__(self, other):
        def or_op(c1, d2):
            c2 = collections.Counter(d2)
            return c1 | c2
        return self._operation(other, or_op)

    def __ror__(self, other):
        return self.__or__(other)

    @same_types
    def __ior__(self, other):
        def ior_op(c1, d2):
            c2 = collections.Counter(d2)
            c1 |= c2
        return self._operation(other, ior_op, update=True)

    @classmethod
    def fromkeys(cls, seq, value=None, **kwargs):
        """This class method is not implemented for :class:`Counter`
        objects.
        """
        raise NotImplementedError
