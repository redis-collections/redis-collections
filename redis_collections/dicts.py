# -*- coding: utf-8 -*-
"""
dicts
~~~~~

The `dicts` module contains standard collections based on Python dictionaries.
Included collections are :class:`Dict`, :class:`Counter`, and
:class:`DefaultDict`.
Each collection stores its items in a Redis
`hash <http://redis.io/commands#hash>`_ structure.

.. note::
    If you need to store mutable values like :class:`list`\s or :class:`set`\s
    in a collection, be sure to enable ``writeback``.
    See :ref:`Synchronization` for more information.

    When storing numeric types (e.g. :class:`float`) as keys, be aware that
    these collections behave slightly differently from standard Python
    dictionaries.
    See :ref:`Hashing` for more information.

"""
from __future__ import division, print_function, unicode_literals

import collections
import operator

import six

from .base import RedisCollection


class Dict(RedisCollection, collections.MutableMapping):
    """
    Collection based on the built-in Python :class:`dict` type.
    Items are stored in a Redis hash structure.
    See Python's `dict documentation
    <https://docs.python.org/3/library/stdtypes.html#mapping-types-dict>`_ for
    usage notes.

    The :func:`viewitems`, :func:`viewkeys`, and :func:`viewvalues` methods
    from Python 2.7's dictionary type are not implemented.
    """

    if six.PY2:
        _pickle_key = RedisCollection._pickle_2
        _unpickle_key = RedisCollection._unpickle_2
    else:
        _pickle_key = RedisCollection._pickle_3
        _unpickle_key = RedisCollection._unpickle

    _pickle_value = RedisCollection._pickle_3

    class __missing_value(object):
        def __repr__(self):
            # Specified here so that the documentation shows a useful string
            # for methods that take __marker as a keyword argument
            return '<missing value>'
    __marker = __missing_value()

    def __init__(self, *args, **kwargs):
        """
        Create a new Dict object.

        If the first argument (*data*) is another mapping type, create the new
        Dict with its items as the initial data.
        Or, If the first argument is an iterable of ``(key, value)`` pairs,
        create the new Dict with those items as the initial data.

        Unlike Python's built-in :class:`dict` type, initial items cannot be
        set using keyword arguments.
        Keyword arguments are passed to the :class:`RedisCollection`
        constructor.

        :param data: Initial data.
        :type data: iterable or mapping

        :param redis: Redis client instance. If not provided, default Redis
                      connection is used.
        :type redis: :class:`redis.StrictRedis`

        :param key: Redis key for the collection. Collections with the same key
                    point to the same data. If not provided, a random
                    string is generated.
        :type key: str

        :param writeback: If ``True``, keep a local cache of changes for
                          storing modifications to mutable values. Changes will
                          be written to Redis after calling the ``sync``
                          method.
        :type writeback: bool
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
        pipe = self.redis if pipe is None else pipe
        return pipe.hlen(self.key)

    def __iter__(self, pipe=None):
        """Return an iterator over the keys of the dictionary."""
        pipe = self.redis if pipe is None else pipe
        for k, v in six.iteritems(self._data(pipe)):
            yield k

    def __contains__(self, key):
        """Return ``True`` if *key* is present, else ``False``."""
        pickled_key = self._pickle_key(key)
        return bool(self.redis.hexists(self.key, pickled_key))

    def __eq__(self, other):
        if not isinstance(other, collections.Mapping):
            return False

        def eq_trans(pipe):
            self_items = self.iteritems(pipe)
            other_items = other.items(pipe) if use_redis else other.items()

            return dict(self_items) == dict(other_items)

        if self._same_redis(other, RedisCollection):
            use_redis = True
            return self._transaction(eq_trans, other.key)
        else:
            use_redis = False
            return self._transaction(eq_trans)

    def getmany(self, *keys):
        """
        Return a list of values corresponding to the keys in the iterable of
        *keys*.
        If a key is not present in the collection, its corresponding value will
        be :obj:`None`.

        .. note::
            This method is not implemented by standard Python dictionary
            classes.
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

        If a subclass of :class:`Dict` defines a method :func:`__missing__`,
        and *key* is not present, the ``d[key]`` operation calls that
        method with the key *key* as argument.
        The ``d[key]`` operation then returns or raises whatever is returned
        or raised by the ``__missing__(key)`` call.
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
        pipe = self.redis if pipe is None else pipe
        items = six.iteritems(pipe.hgetall(self.key))

        return {self._unpickle_key(k): self._unpickle(v) for k, v in items}

    def items(self, pipe=None):
        """Return a copy of the dictionary's list of ``(key, value)`` pairs."""
        return list(self.iteritems(pipe))

    def iteritems(self, pipe=None):
        """Return an iterator over the dictionary's ``(key, value)`` pairs."""
        pipe = self.redis if pipe is None else pipe
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

            return (
                self._unpickle_key(pickled_key), self._unpickle(pickled_value)
            )

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
        """
        Return a new collection with the same items as this one.
        If *key* is specified, create the new collection with the given
        Redis key.
        """
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
            It is possible to specify additional keyword arguments to be passed
            to :func:`__init__` of the new object.
        """
        values = ((key, value) for key in seq)
        return cls(values, **kwargs)

    def scan_items(self):
        """
        Yield each of the ``(key, value)`` pairs from the collection, without
        pulling them all into memory.

        .. warning::
            This method is not available on the dictionary collections provided
            by Python.

            This method may return the same (key, value) pair multiple times.
            See the `Redis SCAN documentation
            <http://redis.io/commands/scan#scan-guarantees>`_ for details.
        """
        for k, v in self.redis.hscan_iter(self.key):
            yield self._unpickle_key(k), self._unpickle(v)

    def _repr_data(self):
        items = ('{}: {}'.format(repr(k), repr(v)) for k, v in self.items())
        return '{{{}}}'.format(', '.join(items))

    def sync(self):
        self.writeback = False
        self._update_helper(self.cache)
        self.cache = {}
        self.writeback = True


class Counter(Dict):
    """
    Collection based on the Python standard library's
    :class:`collections.Counter` type.
    Items are stored in a Redis hash structure.
    See Python's `Counter documentation
    <http://docs.python.org/2/library/collections.html#collections.Counter>`_
    for usage notes.

    Counter inherits from Dict, so see its API documentation for information
    on other methods.

    The :func:`viewitems`, :func:`viewkeys`, and :func:`viewvalues` methods
    from Python 2.7's Counter type are not implemented.
    """

    def __init__(self, *args, **kwargs):
        """
        Create a new Counter object.

        If the first argument (*data*) is another mapping type, create the new
        Counter with the counts of the input items as the initial data.
        Or, If the first argument is an iterable of ``(key, value)`` pairs,
        create the new Counter with those items as the initial data.

        Unlike Python's standard :class:`collections.Counter` type,
        initial items cannot be set using keyword arguments.
        Keyword arguments are passed to the :class:`RedisCollection`
        constructor.

        :param data: Initial data.
        :type data: iterable or mapping

        :param redis: Redis client instance. If not provided, default Redis
                      connection is used.
        :type redis: :class:`redis.StrictRedis`

        :param key: Redis key for the collection. Collections with the same key
                    point to the same data. If not provided, a random
                    string is generated.
        :type key: str

        :param writeback: If ``True``, keep a local cache of changes for
                          storing modifications to mutable values. Changes will
                          be written to Redis after calling the ``sync``
                          method.
        :type writeback: bool
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
                data.update(collections.Counter(other.__iter__(pipe)))
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

    def __pos__(self):
        return self._op_helper(None, operator.pos)

    def __neg__(self):
        return self._op_helper(None, operator.neg)


class DefaultDict(Dict):
    """
    Collection based on the Python standard library's
    :class:`collections.defaultdict` type.
    Items are stored in a Redis hash structure.
    See Python's `defaultdict documentation
    <https://docs.python.org/3/library/collections.html#collections.defaultdict>`_
    for usage notes.

    DefaultDict inherits from Dict, so see its API documentation for
    information on other methods.

    The :func:`viewitems`, :func:`viewkeys`, and :func:`viewvalues` methods
    from Python 2.7's Counter type are not implemented.
    """

    def __init__(self, *args, **kwargs):
        """
        Create a new DefaultDict object.

        The first argument provides the initial value for the
        ``default_factory`` attribute; it defaults to ``None``.
        All other arguments are passed to the ``Dict`` constructor.

        Unlike Python's standard :class:`collections.defaultdict` type,
        initial items cannot be set using keyword arguments.
        Keyword arguments are passed to the :class:`RedisCollection`
        constructor via the ``Dict`` constructor.

        :param default_factory: Used to provide default values for missing
                                keys.
        :type default_factory: callable or None
        :param data: Initial data.
        :type data: iterable or mapping
        :param redis: Redis client instance. If not provided, default Redis
                      connection is used.
        :type redis: :class:`redis.StrictRedis`
        :param key: Redis key for the collection. Collections with the same key
                    point to the same data. If not provided, a random
                    string is generated.
        :type key: str
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
        """
        Return a new collection with the same items as this one.
        If *key* is specified, create the new collection with the given
        Redis key.
        """
        other = self.__class__(self.default_factory, redis=self.redis, key=key)
        other.update(self)

        return other
