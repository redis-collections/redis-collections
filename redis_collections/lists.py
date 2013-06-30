# -*- coding: utf-8 -*-
"""
lists
~~~~~

Collections based on list interface.
"""


import collections

from .base import RedisCollection


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

    def __init__(self, *args, **kwargs):
        """
        :param data: Initial data.
        :type data: iterable
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

        .. note::
            :func:`uuid.uuid4` is used for default key generation.
            If you are not satisfied with its `collision
            probability <http://stackoverflow.com/a/786541/325365>`_,
            make your own implementation by subclassing and overriding
            internal method :func:`_create_key`.
        """
        super(List, self).__init__(*args, **kwargs)

    def __len__(self):
        """Length of the sequence."""
        return self.redis.llen(self.key)

    def _data(self, pipe=None):
        redis = pipe if pipe is not None else self.redis
        values = redis.lrange(self.key, 0, -1)
        return (self._unpickle(v) for v in values)

    def __iter__(self):
        """Return an iterator over the sequence."""
        return self._data()

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

    def _get_slice(self, index):
        """Helper for getting a slice."""
        assert isinstance(index, slice)

        def slice_trans(pipe):
            start, stop = self._recalc_slice(index.start, index.stop)
            values = pipe.lrange(self.key, start, stop)
            if index.step:
                # step implemented by pure Python slicing
                values = values[::index.step]
            values = map(self._unpickle, values)

            pipe.multi()
            return self._create_new(values, pipe=pipe)
        return self._transaction(slice_trans)

    def __getitem__(self, index):
        """Returns item of sequence on *index*.
        Origin of indexes is 0. Accepts also slicing.

        .. note::
            Due to implementation on Redis side, ``l[index]`` is not very
            efficient operation. If possible, use :func:`get`. Slicing without
            steps is efficient. Steps are implemented only on Python side.
        """
        if isinstance(index, slice):
            return self._get_slice(index)

        with self.redis.pipeline() as pipe:
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

    def _set_slice(self, index, value):
        """Helper for setting a slice."""
        assert isinstance(index, slice)

        if value:
            # assigning anything else than empty lists not supported
            raise NotImplementedError(self.not_impl_msg)
        self.__delitem__(index)

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
            self._set_slice(index, value)
        else:
            def set_trans(pipe):
                size = pipe.llen(self.key)
                if self._calc_overflow(size, index):
                    raise IndexError(index)
                pipe.multi()
                pipe.lset(self.key, index, self._pickle(value))

            self._transaction(set_trans)

    def _del_slice(self, index):
        """Helper for deleting a slice."""
        assert isinstance(index, slice)

        begin = 0
        end = -1

        if index.step:
            # stepping not supported
            raise NotImplementedError(self.not_impl_msg)

        start, stop = self._recalc_slice(index.start, index.stop)

        if start == begin and stop == end:
            # trim from beginning to end
            self.clear()
            return

        with self.redis.pipeline() as pipe:
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
            self._del_slice(index)
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

    def count(self, value):
        """Returns number of occurences of *value*.

        .. note::
            Implemented only on Python side.
        """
        return list(self._data()).count(value)

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

        self._transaction(insert_trans)

    def _update(self, data, pipe=None):
        super(List, self)._update(data, pipe)
        redis = pipe if pipe is not None else self.redis

        values = map(self._pickle, data)
        redis.rpush(self.key, *values)

    def extend(self, values):
        """*values* are appended at the end of the list. Any iterable
        is accepted.
        """
        if isinstance(values, RedisCollection):
            # wrap into transaction
            def extend_trans(pipe):
                d = values._data(pipe=pipe)  # retrieve
                pipe.multi()
                self._update(d, pipe=pipe)  # store
            self._transaction(extend_trans)
        else:
            self._update(values)

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
        """Returns concatenation of the list and given iterable. New
        :class:`List` instance is returned.
        """
        def add_trans(pipe):
            d1 = list(self._data(pipe=pipe))  # retrieve

            if isinstance(values, RedisCollection):
                d2 = list(values._data(pipe=pipe))  # retrieve
            else:
                d2 = list(values)

            pipe.multi()
            return self._create_new(d1 + d2, pipe=pipe)  # store
        return self._transaction(add_trans)

    def __radd__(self, values):
        return self.__add__(values)

    def __mul__(self, n):
        """Returns *n* copies of the list, concatenated. New :class:`List`
        instance is returned.
        """
        if not isinstance(n, int):
            raise TypeError('Cannot multiply sequence by non-int.')

        def mul_trans(pipe):
            data = list(self._data(pipe=pipe))  # retrieve
            pipe.multi()
            return self._create_new(data * n, pipe=pipe)  # store
        return self._transaction(mul_trans)

    def __rmul__(self, n):
        return self.__mul__(n)

    def _repr_data(self, data):
        return repr(list(data))
