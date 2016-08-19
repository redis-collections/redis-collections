# -*- coding: utf-8 -*-
"""
sets
~~~~~

Collections based on the set interface.
"""
from __future__ import division, print_function, unicode_literals

import collections
from functools import reduce
import operator

import six

from .base import RedisCollection


class Set(RedisCollection, collections.MutableSet):
    """
    Mutable **set** collection aiming to have the same API as the standard
    set type. See Python's `set documentation
    <http://docs.python.org/2/library/stdtypes.html#set>`_ for
    further details. The Redis implementation is based on the
    `set type <http://redis.io/commands#set>`_.
    """

    if six.PY2:
        _pickle = RedisCollection._pickle_2
        _unpickle = RedisCollection._unpickle_2
    else:
        _pickle = RedisCollection._pickle_3

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
        """
        data = args[0] if args else kwargs.pop('data', None)
        super(Set, self).__init__(**kwargs)

        if data:
            self.update(data)

    def _data(self, pipe=None):
        pipe = pipe or self.redis
        return (self._unpickle(x) for x in pipe.smembers(self.key))

    def _repr_data(self):
        items = (repr(v) for v in self.__iter__())
        return '{{{}}}'.format(', '.join(items))

    # Magic methods

    def __contains__(self, value, pipe=None):
        """Test for membership of *value* in the set."""
        pipe = pipe or self.redis
        return bool(pipe.sismember(self.key, self._pickle(value)))

    def __iter__(self, pipe=None):
        """Return an iterator over elements of the set."""
        pipe = pipe or self.redis
        return self._data(pipe)

    def __len__(self, pipe=None):
        """Return cardinality of the set."""
        pipe = pipe or self.redis
        return pipe.scard(self.key)

    # Named methods

    def add(self, value):
        """Add element *value* to the set."""
        # Raise TypeError if value is not hashable
        hash(value)

        self.redis.sadd(self.key, self._pickle(value))

    def copy(self, key=None):
        other = self.__class__(redis=self.redis, key=key)
        other.update(self)

        return other

    def clear(self, pipe=None):
        """Remove all elements from the set."""
        self._clear(pipe)

    def discard(self, value):
        """Remove element *value* from the set if it is present."""
        # Raise TypeError if value is not hashable
        hash(value)

        self.redis.srem(self.key, self._pickle(value))

    def isdisjoint(self, other):
        """
        Return ``True`` if the set has no elements in common with *other*.
        Sets are disjoint if and only if their intersection is the empty set.

        :param other: Any kind of iterable.
        :rtype: boolean
        """
        def isdisjoint_trans_pure(pipe):
            return not pipe.sinter(self.key, other.key)

        def isdisjoint_trans_mixed(pipe):
            self_values = set(self.__iter__(pipe))
            if use_redis:
                other_values = set(other.__iter__(pipe))
            else:
                other_values = set(other)

            return self_values.isdisjoint(other_values)

        if self._same_redis(other):
            return self._transaction(isdisjoint_trans_pure, other.key)
        if self._same_redis(other, RedisCollection):
            use_redis = True
            return self._transaction(isdisjoint_trans_mixed, other.key)

        use_redis = False
        return self._transaction(isdisjoint_trans_mixed)

    def pop(self):
        """
        Remove and return an arbitrary element from the set.
        Raises :exc:`KeyError` if the set is empty.
        """
        result = self.redis.spop(self.key)
        if result is None:
            raise KeyError

        return self._unpickle(result)

    def random_sample(self, k=1):
        """
        Return a *k* length list of unique elements chosen from the set.
        Elements are not removed. Similar to :func:`random.sample` function
        from standard library.

        :param k: Size of the sample, defaults to 1.
        :rtype: :class:`list`

        .. note::
            Argument *k* is supported only for Redis of version 2.6 and higher.
            This method is not available on from the Python :class:`set`.
        """
        if k == 0:
            return []

        if k == 1:
            results = [self.redis.srandmember(self.key)]
        else:
            results = self.redis.srandmember(self.key, k)

        return [self._unpickle(x) for x in results]

    def remove(self, value):
        """
        Remove element *value* from the set. Raises :exc:`KeyError` if it
        is not contained in the set.
        """
        # Raise TypeError if value is not hashable
        hash(value)

        result = self.redis.srem(self.key, self._pickle(value))
        if not result:
            raise KeyError(value)

    # Comparison and set operation helpers

    def _ge_helper(self, other, op, check_type=False):
        if check_type and not isinstance(other, collections.Set):
            raise TypeError

        def ge_trans_pure(pipe):
            if not op(self.__len__(pipe), other.__len__(pipe)):
                return False

            return not pipe.sdiff(other.key, self.key)

        def ge_trans_mixed(pipe):
            len_other = other.__len__(pipe) if use_redis else len(other)
            if not op(self.__len__(pipe), len_other):
                return False

            values = set(other.__iter__(pipe)) if use_redis else set(other)
            return all(self.__contains__(v, pipe=pipe) for v in values)

        if self._same_redis(other):
            return self._transaction(ge_trans_pure, other.key)
        if self._same_redis(other, RedisCollection):
            use_redis = True
            return self._transaction(ge_trans_mixed, other.key)

        use_redis = False
        return self._transaction(ge_trans_mixed)

    def _le_helper(self, other, op, check_type=False):
        if check_type and not isinstance(other, collections.Set):
            raise TypeError

        def le_trans_pure(pipe):
            if not op(self.__len__(pipe), other.__len__(pipe)):
                return False

            return not pipe.sdiff(self.key, other.key)

        def le_trans_mixed(pipe):
            len_other = other.__len__(pipe) if use_redis else len(other)
            if not op(self.__len__(pipe), len_other):
                return False

            values = set(other.__iter__(pipe)) if use_redis else set(other)
            return all(v in values for v in self.__iter__(pipe))

        if self._same_redis(other):
            return self._transaction(le_trans_pure, other.key)
        if self._same_redis(other, RedisCollection):
            use_redis = True
            return self._transaction(le_trans_mixed, other.key)

        use_redis = False
        return self._transaction(le_trans_mixed)

    def _op_update_helper(
        self, others, op, redis_op, update=False, check_type=False
    ):
        if (
            check_type and
            not all(isinstance(x, collections.Set) for x in others)
        ):
                raise TypeError

        def op_update_trans_pure(pipe):
            method = getattr(pipe, redis_op)
            if not update:
                result = method(self.key, *other_keys)
                return {self._unpickle(x) for x in result}

            temp_key = self._create_key()
            pipe.multi()
            method(temp_key, self.key, *other_keys)
            pipe.rename(temp_key, self.key)

        def op_update_trans_mixed(pipe):
            self_values = set(self.__iter__(pipe))
            other_values = []
            for other in others:
                if isinstance(other, RedisCollection):
                    other_values.append(set(other.__iter__(pipe)))
                else:
                    other_values.append(set(other))

            if not update:
                return reduce(op, other_values, self_values)

            new_values = reduce(op, other_values, self_values)
            pipe.multi()
            pipe.delete(self.key)
            for v in new_values:
                pipe.sadd(self.key, self._pickle(v))

        other_keys = []
        all_redis_sets = True
        for other in others:
            if self._same_redis(other):
                other_keys.append(other.key)
            elif self._same_redis(other, RedisCollection):
                other_keys.append(other.key)
                all_redis_sets = False
            else:
                all_redis_sets = False

        if all_redis_sets:
            return self._transaction(op_update_trans_pure, *other_keys)

        return self._transaction(op_update_trans_mixed, *other_keys)

    def _rop_helper(self, other, op):
        if not isinstance(other, collections.Set):
            raise TypeError

        return op(set(other), set(self.__iter__()))

    def _xor_helper(self, other, update=False, check_type=False):
        if check_type and not isinstance(other, collections.Set):
            raise TypeError

        def xor_trans_pure(pipe):
            diff_1_key = self._create_key()
            pipe.sdiffstore(diff_1_key, self.key, other.key)

            diff_2_key = self._create_key()
            pipe.sdiffstore(diff_2_key, other.key, self.key)

            if update:
                pipe.sunionstore(self.key, diff_1_key, diff_2_key)
                ret = None
            else:
                ret = pipe.sunion(diff_1_key, diff_2_key)
                ret = {self._unpickle(x) for x in ret}

            pipe.delete(diff_1_key, diff_2_key)

            return ret

        def xor_trans_mixed(pipe):
            self_values = set(self.__iter__(pipe))
            if use_redis:
                other_values = set(other.__iter__(pipe))
            else:
                other_values = set(other)

            result = self_values ^ other_values

            if update:
                pipe.delete(self.key)
                pipe.sadd(self.key, *(self._pickle(x) for x in result))
                return None

            return result

        if self._same_redis(other):
            return self._transaction(xor_trans_pure, other.key)
        elif self._same_redis(other, RedisCollection):
            use_redis = True
            return self._transaction(xor_trans_mixed, other.key)

        use_redis = False
        return self._transaction(xor_trans_mixed)

    # Intersection

    def __and__(self, other):
        return self._op_update_helper(
            (other,), operator.and_, 'sinter', check_type=True
        )

    def __rand__(self, other):
        return self._rop_helper(other, operator.and_)

    def __iand__(self, other):
        self._op_update_helper(
            (other,),
            operator.and_,
            'sinterstore',
            update=True,
            check_type=True,
        )
        return self

    def intersection(self, *others):
        """
        Return a new set with elements common to the set and all *others*.

        :param others: Iterables, each one as a single positional argument.
        :rtype: :class:`set`

        .. note::
            The same behavior as at :func:`union` applies.
        """
        return self._op_update_helper(tuple(others), operator.and_, 'sinter')

    def intersection_update(self, *others):
        """
        Update the set, keeping only elements found in it and all *others*.

        :param others: Iterables, each one as a single positional argument.
        :rtype: None

        .. note::
            The same behavior as at :func:`difference_update` applies.
        """
        return self._op_update_helper(
            tuple(others), operator.and_, 'sinterstore', update=True
        )

    # Comparison

    def __ge__(self, other):
        return self._ge_helper(other, operator.ge, check_type=True)

    def issuperset(self, other):
        """
        Test whether every element in other is in the set.

        :param other: Any kind of iterable.
        :rtype: boolean
        """
        return self._ge_helper(other, operator.ge)

    def __gt__(self, other):
        return self._ge_helper(other, operator.gt, check_type=True)

    def __eq__(self, other):
        return self._le_helper(other, operator.eq, check_type=True)

    def __le__(self, other):
        return self._le_helper(other, operator.le, check_type=True)

    def issubset(self, other):
        """
        Test whether every element in the set is in *other*.

        :param other: Any kind of iterable.
        :rtype: boolean
        """
        return self._le_helper(other, operator.le)

    def __lt__(self, other):
        return self._le_helper(other, operator.lt)

    # Union

    def __or__(self, other):
        return self._op_update_helper(
            (other,), operator.or_, 'sunion', check_type=True
        )

    def __ror__(self, other):
        return self._rop_helper(other, operator.or_)

    def __ior__(self, other):
        self._op_update_helper(
            (other,), operator.or_, 'sunionstore', update=True, check_type=True
        )
        return self

    def union(self, *others):
        """
        Return a new set with elements from the set and all *others*.

        :param others: Iterables, each one as a single positional argument.
        :rtype: :class:`set`

        .. note::
            If all *others* are :class:`Set` instances, the operation
            is performed completely in Redis. Otherwise, values are retrieved
            from Redis and the operation is performed in Python.
        """
        return self._op_update_helper(tuple(others), operator.or_, 'sunion')

    def update(self, *others):
        """
        Update the set, adding elements from all *others*.

        :param others: Iterables, each one as a single positional argument.
        :rtype: None

        .. note::
            If all *others* are :class:`Set` instances, the operation
            is performed completely in Redis. Otherwise, values are retrieved
            from Redis and the operation is performed in Python.
        """
        return self._op_update_helper(
            tuple(others), operator.or_, 'sunionstore', update=True
        )

    # Difference

    def __sub__(self, other):
        return self._op_update_helper(
            (other,), operator.sub, 'sdiff', check_type=True
        )

    def __rsub__(self, other):
        return self._rop_helper(other, operator.sub)

    def __isub__(self, other):
        self._op_update_helper(
            (other,), operator.sub, 'sdiffstore', update=True, check_type=True
        )
        return self

    def difference(self, *others):
        """
        Return a new set with elements in the set that are not in the *others*.

        :param others: Iterables, each one as a single positional argument.
        :rtype: :class:`set`

        .. note::
            The same behavior as at :func:`union` applies.
        """
        return self._op_update_helper(tuple(others), operator.sub, 'sdiff')

    def difference_update(self, *others):
        """
        Update the set, removing elements found in *others*.

        :param others: Iterables, each one as a single positional argument.
        :rtype: None

        .. note::
            The same behavior as at :func:`update` applies.
        """
        return self._op_update_helper(
            tuple(others), operator.sub, 'sdiffstore', update=True
        )

    # Symmetric difference

    def __xor__(self, other):
        return self._xor_helper(other, check_type=True)

    def __ixor__(self, other):
        self._xor_helper(other, update=True, check_type=True)
        return self

    def symmetric_difference(self, other):
        """
        Return a new set with elements in either the set or *other* but not
        both.

        :param other: Any kind of iterable.
        :rtype: :class:`set`

        .. note::
            The same behavior as at :func:`union` applies.
        """
        return self._xor_helper(other)

    def symmetric_difference_update(self, other):
        """
        Update the set, keeping only elements found in either set, but not
        in both.

        :param other: Any kind of iterable.
        :rtype: None

        .. note::
            The same behavior as at :func:`update` applies.
        """
        self._xor_helper(other, update=True)
        return self
