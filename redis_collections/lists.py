# -*- coding: utf-8 -*-
"""
lists
~~~~~

Collections based on the list interface.
"""
from __future__ import division, print_function, unicode_literals

import collections
import itertools
import uuid

import six
from redis import ResponseError

from .base import RedisCollection


class List(RedisCollection, collections.MutableSequence):
    """Mutable **sequence** collection aiming to have the same API as the
    standard sequence type, :class:`list`. See Python's `list documentation
    <http://docs.python.org/2/library/functions.html#list>`_ for
    further details. The Redis implementation is based on the
    `list type <http://redis.io/commands#list>`_.
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
        :param writeback: If ``True`` keep a local cache of changes for storing
                          modifications to mutable values. Changes will be
                          written to Redis after calling the ``sync`` method.
        :type key: bool
        """
        data = args[0] if args else kwargs.pop('data', None)
        writeback = kwargs.pop('writeback', False)
        super(List, self).__init__(**kwargs)

        self.__marker = uuid.uuid4().hex
        self.writeback = writeback
        self.cache = {}

        if data:
            self.extend(data)

    def _normalize_index(self, index, pipe=None):
        """Convert negative indexes into their positive equivalents."""
        pipe = pipe or self.redis
        len_self = pipe.llen(self.key)
        positive_index = index if index >= 0 else len_self + index

        return len_self, positive_index

    def _normalize_slice(self, index, pipe=None):
        """Given a :obj:`slice` *index*, return a 4-tuple
        ``(start, stop, step, fowrward)``. The first three items can be used
        with the ``range`` function to retrieve the values associated with the
        slice; the last item indicates the direction.
        """
        if index.step == 0:
            raise ValueError
        pipe = pipe or self.redis

        len_self = pipe.llen(self.key)

        step = index.step or 1
        forward = step > 0
        step = abs(step)

        if index.start is None:
            start = 0 if forward else len_self - 1
        elif index.start < 0:
            start = max(len_self + index.start, 0)
        else:
            start = min(index.start, len_self)

        if index.stop is None:
            stop = len_self if forward else -1
        elif index.stop < 0:
            stop = max(len_self + index.stop, 0)
        else:
            stop = min(index.stop, len_self)

        if not forward:
            start, stop = min(stop + 1, len_self), min(start + 1, len_self)

        return start, stop, step, forward, len_self

    def _pop_left(self):
        """Retrieve a value from the 0 index, remove it, and return it."""
        pickled_value = self.redis.lpop(self.key)
        if pickled_value is None:
            raise IndexError
        value = self._unpickle(pickled_value)

        if self.writeback:
            value = self.cache.get(0, value)
            items = six.iteritems(self.cache)
            self.cache = {i - 1: v for i, v in items if i != 0}

        return value

    def _pop_right(self):
        """Retrieve a value from the -1 index, remove it, and return it."""
        if not self.writeback:
            pickled_value = self.redis.rpop(self.key)
            if pickled_value is None:
                raise IndexError
            return self._unpickle(pickled_value)

        # If writeback is enabled we'll need the size of the list; compute that
        # in a transaction
        def pop_right_trans(pipe):
            len_self, cache_index = self._normalize_index(-1, pipe)
            if len_self == 0:
                raise IndexError
            pickled_value = pipe.rpop(self.key)
            value = self.cache.get(cache_index, self._unpickle(pickled_value))
            items = six.iteritems(self.cache)
            self.cache = {i: v for i, v in items if i != cache_index}

            return value

        return self._transaction(pop_right_trans)

    def _pop_middle(self, index):
        """Retrieve the value at *index*, remove it, and return it."""
        def pop_middle_trans(pipe):
            len_self, cache_index = self._normalize_index(index, pipe)
            if (cache_index < 0) or (cache_index >= len_self):
                raise IndexError

            # Retrieve the value at index, then overwrite it with a special
            # marker, and then remove the marker.
            value = self._unpickle(pipe.lindex(self.key, index))
            pipe.multi()
            pipe.lset(self.key, index, self.__marker)
            pipe.lrem(self.key, 1, self.__marker)

            if self.writeback:
                value = self.cache.get(cache_index, value)
                new_cache = {}
                for i, v in six.iteritems(self.cache):
                    if i < cache_index:
                        new_cache[i] = v
                    elif i > cache_index:
                        new_cache[i - 1] = v
                self.cache = new_cache

            return value

        return self._transaction(pop_middle_trans)

    def _del_slice(self, index):
        """Delete the values associated with :obj:`slice` *index*."""
        def del_slice_trans(pipe):
            start, stop, step, forward, len_self = self._normalize_slice(
                index, pipe
            )

            # Empty slice: nothing to do
            if start == stop:
                return

            # Write back the cache before making changes
            if self.writeback:
                self._sync_helper(pipe)

            # Steps must be done index by index
            if index.step is not None:
                pipe.multi()
                for i in list(six.moves.xrange(len_self))[index]:
                    pipe.lset(self.key, i, self.__marker)
                pipe.lrem(self.key, 0, self.__marker)
            # Slice covers entire range: delete the whole list
            elif start == 0 and stop == len_self:
                self.clear(pipe)
            # Slice starts on the left: keep the right
            elif start == 0 and stop != len_self:
                pipe.ltrim(self.key, stop, -1)
            # Slice stops on the right: keep the left
            elif start != 0 and stop == len_self:
                pipe.ltrim(self.key, 0, start - 1)
            # Slice starts and ends in the middle
            else:
                left_values = pipe.lrange(self.key, 0, start - 1)
                right_values = pipe.lrange(self.key, stop, -1)
                pipe.delete(self.key)
                all_values = itertools.chain(left_values, right_values)
                pipe.rpush(self.key, *all_values)

        return self._transaction(del_slice_trans)

    def __delitem__(self, index):
        """Delete the item at **index**."""
        if isinstance(index, slice):
            return self._del_slice(index)

        if index == 0:
            self._pop_left()
        elif index == -1:
            self._pop_right()
        else:
            self._pop_middle(index)

    def _get_slice(self, index):
        """
        Return the values specified by :obj:`slice` *index* as a :obj:``list``.
        """
        def get_slice_trans(pipe):
            start, stop, step, forward, len_self = self._normalize_slice(
                index, pipe
            )

            if start == stop:
                return []

            ret = []
            redis_values = pipe.lrange(self.key, start, max(stop - 1, 0))
            for i, v in enumerate(redis_values, start):
                ret.append(self.cache.get(i, self._unpickle(v)))

            if not forward:
                ret = reversed(ret)

            if step != 1:
                ret = itertools.islice(ret, None, None, step)

            return list(ret)

        return self._transaction(get_slice_trans)

    def __getitem__(self, index):
        """
        If *index* is an :obj:``int``, return the value at that index.
        If *index* is a :obj:``slice``, return the values from that slice
        as a :obj:``list``.
        """
        if isinstance(index, slice):
            return self._get_slice(index)

        # If writeback is off we can just query Redis, since its indexing
        # scheme matches Python's. If the index is out of range we get None.
        if not self.writeback:
            pickled_value = self.redis.lindex(self.key, index)
            if pickled_value is None:
                raise IndexError

            return self._unpickle(pickled_value)

        # If writeback is on we'll need to know the size of the list,
        # so we'll need to use a transaction
        def getitem_trans(pipe):
            len_self, cache_index = self._normalize_index(index, pipe)

            if (cache_index < 0) or (cache_index >= len_self):
                raise IndexError

            if cache_index in self.cache:
                return self.cache[cache_index]

            value = self._unpickle(pipe.lindex(self.key, index))
            self.cache[cache_index] = value
            return value

        return self._transaction(getitem_trans)

    def _data(self, pipe=None):
        """
        Return a :obj:``list`` of all values from Redis
        (without checking the local cache).
        """
        pipe = pipe or self.redis
        return [self._unpickle(v) for v in pipe.lrange(self.key, 0, -1)]

    def __iter__(self, pipe=None):
        """
        Return a :obj:``list`` of all values from Redis (overriding those with
        values from the local cache)
        """
        return (self.cache.get(i, v) for i, v in enumerate(self._data(pipe)))

    def __len__(self, pipe=None):
        """Return the length of this collection."""
        pipe = pipe or self.redis
        return pipe.llen(self.key)

    def __reversed__(self):
        """
        Return an iterator over this collection's items in reverse order.
        """
        return reversed(list(self.__iter__()))

    def _set_slice(self, index, value):
        """
        Set the values for the indexes associated with :obj:``slice`` *index*
        to the contents of the iterable *value*.
        """
        def set_slice_trans(pipe):
            start, stop, step, forward, len_self = self._normalize_slice(
                index, pipe
            )

            # Write back the cache before making changes
            if self.writeback:
                self._sync_helper(pipe)

            # Loop through each index for slices with steps
            if index.step is not None:
                new_values = list(value)
                change_indexes = six.moves.xrange(start, stop, step)
                if len(new_values) != len(change_indexes):
                    raise ValueError
                for i, v in six.moves.zip(change_indexes, new_values):
                    pipe.lset(self.key, i, self._pickle(v))
            # For slices without steps retrieve the items to the left and right
            # of the slice, clear the collection, then re-insert the items
            # with the new values in the middle.
            else:
                if start == 0:
                    left_values = []
                else:
                    left_values = pipe.lrange(self.key, 0, start - 1)

                middle_values = (self._pickle(v) for v in value)

                if stop == len_self:
                    right_values = []
                else:
                    right_values = pipe.lrange(self.key, stop, -1)

                pipe.delete(self.key)
                all_values = itertools.chain(
                    left_values, middle_values, right_values
                )
                pipe.rpush(self.key, *all_values)

        return self._transaction(set_slice_trans)

    def __setitem__(self, index, value):
        """
        If *index* is an :obj:``int``, set the value for that index to *value*.
        If *index* is a :obj:``slice``, set the values for the indexes
        associated with that slice  to the contents of the iterable *value*.
        """
        if isinstance(index, slice):
            return self._set_slice(index, value)

        with self.redis.pipeline() as pipe:
            if self.writeback:
                __, cache_index = self._normalize_index(index, pipe)

            pipe.lset(self.key, index, self._pickle(value))

            try:
                pipe.execute()
            except ResponseError:
                raise IndexError

            if self.writeback:
                self.cache[cache_index] = value

    def append(self, value):
        """Insert *value* at the end of this collection."""
        len_self = self.redis.rpush(self.key, self._pickle(value))

        if self.writeback:
            self.cache[len_self - 1] = value

    def clear(self, pipe=None):
        """Delete all values from this collection."""
        self._clear(pipe)

        if self.writeback:
            self.cache.clear()

    def copy(self, key=None):
        """
        Return a new :obj:``List`` with the specified *key*. The new collection
        will have the same values as this collection.
        """
        other = self.__class__(
            redis=self.redis, key=key, writeback=self.writeback
        )
        other.extend(self)

        return other

    def count(self, value):
        """
        Return the number of occurences of *value*.

        .. note::
            Counting is implemented in Python.
        """
        return sum(1 for v in self.__iter__() if v == value)

    def extend(self, other):
        """
        Adds the values from the iterable *other* to the end of this
        collection.
        """
        def extend_trans(pipe):
            if use_redis:
                values = list(other.__iter__(pipe))
            else:
                values = other
            len_self = pipe.rpush(self.key, *(self._pickle(v) for v in values))
            if self.writeback:
                for i, v in enumerate(values, len_self - len(values)):
                    self.cache[i] = v

        if self._same_redis(other, RedisCollection):
            use_redis = True
            self._transaction(extend_trans, other.key)
        else:
            use_redis = False
            self._transaction(extend_trans)

    def index(self, value, start=None, stop=None):
        """
        Return the index of the first occurence of *value*.
        If *start* or *stop* are provided, return the smallest
        index such that ``s[index] == value`` and ``start <= index < stop``.
        """
        def index_trans(pipe):
            len_self, normal_start = self._normalize_index(start or 0, pipe)
            __, normal_stop = self._normalize_index(stop or len_self, pipe)
            for i, v in enumerate(self.__iter__(pipe=pipe)):
                if v == value:
                    if i < normal_start:
                        continue
                    if i >= normal_stop:
                        break
                    return i
            raise ValueError

        return self._transaction(index_trans)

    def insert(self, index, value):
        """
        Insert *value* into the collection at *index*.
        """
        # Easy case: insert on the left
        if index == 0:
            self.redis.lpush(self.key, self._pickle(value))
            if self.writeback:
                self.cache = {k + 1: v for k, v in six.iteritems(self.cache)}
                self.cache[0] = value
        # Difficult case: insert in the middle.
        # Retrieve everything from ``index`` up to the end, insert ``value``
        # on the right, then re-insert the retrieved items on the right
        else:
            def insert_middle_trans(pipe):
                __, cache_index = self._normalize_index(index, pipe)
                right_values = pipe.lrange(self.key, cache_index, -1)
                pipe.multi()
                pipe.ltrim(self.key, 0, cache_index - 1)
                pipe.rpush(self.key, self._pickle(value), *right_values)
                if self.writeback:
                    new_cache = {}
                    for i, v in six.iteritems(self.cache):
                        if i < cache_index:
                            new_cache[i] = v
                        elif i >= cache_index:
                            new_cache[i + 1] = v
                    new_cache[cache_index] = value
                    self.cache = new_cache
            self._transaction(insert_middle_trans)

    def pop(self, index=-1):
        """
        Retrieve the value at *index*, remove it from the collection, and
        return it.
        """
        if index == 0:
            return self._pop_left()
        elif index == -1:
            return self._pop_right()
        else:
            return self._pop_middle(index)

    def remove(self, value):
        """Remove the first occurence of *value*."""
        # If we're caching, we'll need to synchronize before removing.
        with self.redis.pipeline() as pipe:
            if self.writeback:
                self._sync_helper(pipe)
            pipe.lrem(self.key, 1, self._pickle(value))
            results = pipe.execute()

        if results[-1] == 0:
            raise ValueError

    def reverse(self):
        """
        Reverses the items of this collection "in place" (only two values are
        retrieved from Redis at a time).
        """
        def reverse_trans(pipe):
            if self.writeback:
                self._sync_helper(pipe)

            n = pipe.llen(self.key)
            for i in six.moves.xrange(n // 2):
                left = pipe.lindex(self.key, i)
                right = pipe.lindex(self.key, n - i - 1)
                pipe.lset(self.key, i, right)
                pipe.lset(self.key, n - i - 1, left)

        self._transaction(reverse_trans)

    def sort(self, key=None, reverse=False):
        """
        Sort the items of this collection according to the optional callable
        *key*. If *reverse* is set then the sort order is reversed.

        .. note::
            This sort requires all items to be retrieved from Redis and stored
            in memory.
         """
        def sort_trans(pipe):
            values = list(self.__iter__(pipe))
            values.sort(key=key, reverse=reverse)

            pipe.multi()
            pipe.delete(self.key)
            pipe.rpush(self.key, *(self._pickle(v) for v in values))

            if self.writeback:
                self.cache = {}

        return self._transaction(sort_trans)

    def __add__(self, other):
        return list(self.__iter__()) + list(other)

    def __iadd__(self, other):
        self.extend(other)
        return self

    def __mul__(self, times):
        if not isinstance(times, six.integer_types):
            raise TypeError

        return list(self.__iter__()) * times

    def __rmul__(self, times):
        return self.__mul__(times)

    def __imul__(self, times):
        if not isinstance(times, six.integer_types):
            raise TypeError

        # If multiplying by 1 there's no work to do
        if times == 1:
            return self

        def imul_trans(pipe):
            # If multiplying by 0 or a negative number all values are deleted
            if times <= 0:
                self.clear(pipe)

            # Synchronize the cache before writing
            if self.writeback:
                self._sync_helper(pipe)

            # Pull in pickled values
            pickled_values = pipe.lrange(self.key, 0, -1)
            pipe.multi()

            # Write the values repeatedly
            for __ in six.moves.xrange(times - 1):
                pipe.rpush(self.key, *pickled_values)

        self._transaction(imul_trans)
        return self

    def _repr_data(self):
        items = (repr(v) for v in self.__iter__())
        return '[{}]'.format(', '.join(items))

    def _sync_helper(self, pipe):
        for i, v in six.iteritems(self.cache):
            pipe.lset(self.key, i, self._pickle(v))

        self.cache = {}

    def sync(self):
        def sync_trans(pipe):
            pipe.multi()
            self._sync_helper(pipe)

        self._transaction(sync_trans)
