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
        D_subset = {}
        for D in self.redis.hmget(self.key, *(hash(k) for k in keys)):
            if D is not None:
                D_subset.update(self._unpickle(D))

        return [D_subset.get(key) for key in keys]

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
        """Returns a Python dictionary with the same values as this object"""
        redis = self.redis if pipe is None else pipe

        ret = {}
        for D in six.itervalues(redis.hgetall(self.key)):
            ret.update(self._unpickle(D))

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
                pipe.hset(self.key, key_hash, self._pickle(D))
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

            pipe.multi()
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

    def clear(self):
        self.redis.delete(self.key)

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
        Note that this :class:`Counter` does not implement
        methods :func:`viewitems`, :func:`viewkeys`, and :func:`viewvalues`,
        which are available in Python 2.7's version.
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

            if use_redis:
                for D in six.itervalues(pipe.hgetall(other.key)):
                    data.update(self._unpickle(D))
            else:
                data.update(other)

            D_load = {}
            for key, value in six.iteritems(data):
                key_hash = hash(key)
                D_load.setdefault(key_hash, {})
                D_load[key_hash][key] = op(self.get(key, 0), value)

            pipe.multi()
            for key_hash, D in six.iteritems(D_load):
                pipe.hset(self.key, key_hash, self._pickle(D))

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
            if isinstance(other, Dict):
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
            if isinstance(other, Dict):
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
            self_counter = collections.Counter(self._data(pipe))

            # If `other` is also Redis-backed we'll want to pull its values
            # with the same transaction-provided pipeline as for `self`.
            if use_redis:
                other_counter = collections.Counter(other._data(pipe))
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
            D_load = {}
            for key, value in six.iteritems(result):
                key_hash = hash(key)
                D_load.setdefault(key_hash, {})
                D_load[key_hash][key] = value

            pipe.multi()
            pipe.delete(self.key)
            for key_hash, D in six.iteritems(D_load):
                pipe.hset(self.key, key_hash, self._pickle(D))

        if other is None:
            use_redis = False
            result = self._transaction(op_trans, None)
        elif isinstance(other, Counter):
            use_redis = True
            result = self._transaction(op_trans, other.key)
        elif isinstance(other, collections.Counter):
            use_redis = False
            result = self._transaction(op_trans)
        else:
            raise TypeError('Unsupported type {}'.format(type(other)))

        if inplace:
            return self
        else:
            new_instance = self.__class__(redis=self.redis)
            new_instance.update(result)
            return new_instance

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
