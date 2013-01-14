# -*- coding: utf-8 -*-
"""
sets
~~~~
"""


import operator
import itertools
import collections

from .base import RedisCollection


class Set(RedisCollection, collections.MutableSet):
    """Mutable **set** collection aiming to have the same API as the standard
    set type. See `set
    <http://docs.python.org/2/library/stdtypes.html#set>`_ for
    further details. The Redis implementation is based on the
    `set <http://redis.io/commands#set>`_ type.
    """

    def __init__(self, *args, **kwargs):
        """
        :param data: Initial data.
        :type data: iterable
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

        .. note::
            :func:`uuid.uuid4` is used for default ID generation.
            If you are not satisfied with its `collision
            probability <http://stackoverflow.com/a/786541/325365>`_,
            make your own implementation by subclassing and overriding method
            :func:`_create_new_id`.
        """
        super(Set, self).__init__(*args, **kwargs)

    def __len__(self):
        """Return cardinality of the set."""
        return self.redis.scard(self.key)

    def _data(self, pipe=None):
        redis = pipe or self.redis
        return (self._unpickle(v) for v in redis.smembers(self.key))

    def __iter__(self):
        """Return an iterator over elements of the set."""
        return self._data()

    def __contains__(self, elem):
        """Test for membership of *elem* in the set."""
        return self.redis.sismember(self.key, self._pickle(elem))

    def add(self, elem):
        """Add element *elem* to the set."""
        self.redis.sadd(self.key, self._pickle(elem))

    def discard(self, elem):
        """Remove element *elem* from the set if it is present."""
        self.redis.srem(self.key, self._pickle(elem))

    def remove(self, elem):
        """Remove element *elem* from the set. Raises :exc:`KeyError` if elem
        is not contained in the set.
        """
        removed_count = self.redis.srem(self.key, self._pickle(elem))
        if not removed_count:
            raise KeyError(elem)

    def pop(self):
        """Remove and return an arbitrary element from the set.
        Raises :exc:`KeyError` if the set is empty.
        """
        with self.redis.pipeline() as pipe:
            pipe.scard(self.key)
            pipe.spop(self.key)
            size, elem = pipe.execute()

        if not size:
            raise KeyError
        return self._unpickle(elem)

    def random_sample(self, k=1):
        """Return a *k* length list of unique elements chosen from the set.
        Elements are not removed. Similar to :func:`random.sample` function
        from standard library.

        :param k: Size of the sample, defaults to 1.
        :rtype: :class:`list`

        .. note::
            Argument *k* is supported only for Redis of version 2.6 and higher.
        """
        if k < 1:
            return []
        if k == 1:
            elements = [self.redis.srandmember(self.key)]
        else:
            elements = self.redis.srandmember(self.key, k)
        return map(self._unpickle, elements)

    def _are_redis_sets(self, iterables):
        """Helper method deciding whether given *iterables* are all instances
        of the :class:`Set` class.

        :param iterables: Any iterable of iterables.
        :rtype: boolean
        """
        is_redis_set = lambda i: isinstance(i, Set)
        return all(map(is_redis_set, iterables))

    def _operation(self, op, redisop, redisopstore, others, type=None,
                   update=False):
        """Helper method implementing standard operations.

        :param op: Name of the operation as known from standard :class:`set`
                   module. For example ``difference`` or
                   ``intersection_update``.
        :param redisop: Lowercase name of Redis command implementing
                        the operation.
        :param redisopstore: Lowercase name of the *STORE* version
                             of *redisop*.
        :param others: Iterable of one or more iterables, which are part
                       of this operation.
        :param type: Class object specifying the type to be used for result
                     of the operation. If *update* is :obj:`True`, this
                     argument is ignored as update operations have no return
                     values.
        :param update: If :obj:`True`, operation is considered to be *update*.
                       That means it affects current object given in *self* and
                       does not return any value.

        .. warning::
            **Operation is not atomic.**
        """
        return_type = self.__class__ if update else (type or Set)
        return_id = self.id if update else None

        if self._are_redis_sets(others):
            keys = [other.key for other in others]

            if issubclass(return_type, self.__class__):
                # operation can be performed in Redis completely
                return_obj = self._create_new(type=return_type, id=return_id)
                fn = getattr(self.redis, redisopstore)
                fn(return_obj.key, self.key, *keys)
                return return_obj
            else:
                # operation can be performed in Redis and returned to Python
                fn = getattr(self.redis, redisop)
                elements = fn(self.key, *keys)
                return self._create_new(elements, type=return_type,
                                        id=return_id)

        # else do it in Python completely,
        # simulating the same operation on standard set
        python_set = set(self)
        fn = getattr(python_set, op)
        result = fn(*map(frozenset, others))
        elements = python_set if update else result

        return self._create_new(elements, type=return_type, id=return_id)

    def _binary_operation(self, op, redisopstore, other, update=False,
                          right=False):
        """Helper method implementing standard **binary** operations.

        :param op: Name of the operation as known from :mod:`operator` module.
        :param redisopstore: Lowercase name of the *STORE* version
                             of Redis command implementing the operation.
        :param other: Iterable, which is operand in this operation.
        :param update: If :obj:`True`, operation is considered to be *update*.
                       That means it affects current object given in *self* and
                       does not return any value.
        :param right: Specifies whether the operation is in reversed mode,
                      where *self* is the right operand and *other* is
                      the left one.

        .. warning::
            **Operation is not atomic.**
        """
        return_id = self.id if update else None

        if not isinstance(other, collections.Set):  # collections.Set is ABC
            raise TypeError('Only sets are supported as operand types.')

        if right:
            left_operand = other
            right_operand = self
        else:
            left_operand = self
            right_operand = other

        return_type = left_operand.__class__

        if isinstance(other, Set):
            return_obj = self._create_new(id=return_id, type=return_type)
            fn = getattr(self.redis, redisopstore)
            fn(return_obj.key, left_operand.key, right_operand.key)
            return return_obj

        fn = getattr(operator, op)  # standard operator module
        elements = fn(frozenset(left_operand), frozenset(right_operand))
        return self._create_new(elements, id=return_id, type=return_type)

    def difference(self, *others, **kwargs):
        """Return a new set with elements in the set that are
        not in the *others*.

        :param others: Iterables, each one as a single positional argument.
        :param type: Keyword argument, type of result, defaults to the same
                     type as collection (:class:`Set`, if not inherited).
        :rtype: :class:`Set` or collection of type specified in *type* argument

        .. note::
            If all *others* are :class:`Set` instances, operation
            is performed completely in Redis. If *type* is provided,
            operation is still performed in Redis, but results are sent
            back to Python and returned with corresponding type. All other
            combinations are performed only on Python side. All other
            combinations are performed in Python and results are sent
            to Redis. See examples::

                s1 = Set([1, 2])
                s2 = Set([2, 3])
                s3 = set([2, 3])  # built-in set

                # Redis (whole operation)
                s1.difference(s2, s2, s2)  # = Set

                # Python (operation) → Redis (new key with Set)
                s1.difference(s3)  # = Set

                # Python (operation) → Redis (new key with Set)
                s1.difference(s2, s3, s2)  # = Set

                # Redis (operation) → Python (type conversion)
                s1.difference(s2, type=set)  # = set

                # Redis (operation) → Python (type conversion)
                s1.difference(s2, type=list)  # = list

                # Redis (operation) → Python → Redis (new key with List)
                s1.difference(s2, type=List)  # = List

        .. warning::
            **Operation is not atomic.**
        """
        return self._operation('difference', 'sdiff', 'sdiffstore',
                               others, type=kwargs.get('type'))

    def __sub__(self, other):
        """Return a new set with elements in the set that are
        not in the *other*.

        :param other: Set object (instance of :class:`collections.Set`
                      ABC, so built-in sets and frozensets are also accepted),
                      otherwise :exc:`TypeError` is raised.
        :rtype: type of the first operand

        .. note::
            If *other* is instance of :class:`Set`, operation
            is performed completely in Redis. Otherwise it's performed
            in Python and results are sent to Redis.

        .. warning::
            **Operation is not atomic.**
        """
        return self._binary_operation('sub', 'sdiffstore', other)

    def __rsub__(self, other):
        return self._binary_operation('sub', 'sdiffstore', other, right=True)

    def difference_update(self, *others):
        """Update the set, removing elements found in *others*.

        :param others: Iterables, each one as a single positional argument.
        :rtype: None

        .. note::
            If all *others* are :class:`Set` instances, operation
            is performed completely in Redis. Otherwise it's performed
            in Python and results are sent to Redis. See examples::

                s1 = Set([1, 2])
                s2 = Set([2, 3])
                s3 = set([2, 3])  # built-in set

                # Redis (whole operation)
                s1.difference_update(s2, s2)  # = None

                # Python (operation) → Redis (update)
                s1.difference(s3)  # = None

                # Python (operation) → Redis (update)
                s1.difference(s2, s3, s2)  # = None

        .. warning::
            **Operation is not atomic.**
        """
        return self._operation('difference_update', 'sdiff', 'sdiffstore',
                               others, update=True)

    def __isub__(self, other):
        """Update the set, removing elements found in *other*.

        :param other: Set object (instance of :class:`collections.Set`
                      ABC, so built-in sets and frozensets are also accepted),
                      otherwise :exc:`TypeError` is raised.
        :rtype: None

        .. note::
            If *other* is instance of :class:`Set`, operation
            is performed completely in Redis. Otherwise it's performed
            in Python and results are sent to Redis.

        .. warning::
            **Operation is not atomic.**
        """
        return self._binary_operation('sub', 'sdiffstore', other,
                                      update=True)

    def intersection(self, *others, **kwargs):
        """Return a new set with elements common to the set and all *others*.

        :param others: Iterables, each one as a single positional argument.
        :param type: Keyword argument, type of result, defaults to the same
                     type as collection (:class:`Set`, if not inherited).
        :rtype: :class:`Set` or collection of type specified in *type* argument

        .. note::
            The same behavior as at :func:`difference` applies.

        .. warning::
            **Operation is not atomic.**
        """
        return self._operation('intersection', 'sinter', 'sinterstore',
                               others, type=kwargs.get('type'))

    def __and__(self, other):
        """Return a new set with elements common to the set and the *other*.

        :param other: Set object (instance of :class:`collections.Set`
                      ABC, so built-in sets and frozensets are also accepted),
                      otherwise :exc:`TypeError` is raised.
        :rtype: type of the first operand

        .. note::
            The same behavior as at :func:`__sub__` applies.

        .. warning::
            **Operation is not atomic.**
        """
        return self._binary_operation('and_', 'sinterstore', other)

    def __rand__(self, other):
        return self._binary_operation('and_', 'sinterstore', other, right=True)

    def intersection_update(self, *others):
        """Update the set, keeping only elements found in it and all *others*.

        :param others: Iterables, each one as a single positional argument.
        :rtype: None

        .. note::
            The same behavior as at :func:`difference_update` applies.

        .. warning::
            **Operation is not atomic.**
        """
        return self._operation('intersection_update', 'sinter', 'sinterstore',
                               others, update=True)

    def __iand__(self, other):
        """Update the set, keeping only elements found in it and the *other*.

        :param other: Set object (instance of :class:`collections.Set`
                      ABC, so built-in sets and frozensets are also accepted),
                      otherwise :exc:`TypeError` is raised.
        :rtype: None

        .. note::
            The same behavior as at :func:`__isub__` applies.

        .. warning::
            **Operation is not atomic.**
        """
        return self._binary_operation('and_', 'sinterstore', other,
                                      update=True)

    def union(self, *others, **kwargs):
        """Return a new set with elements from the set and all *others*.

        :param others: Iterables, each one as a single positional argument.
        :param type: Keyword argument, type of result, defaults to the same
                     type as collection (:class:`Set`, if not inherited).
        :rtype: :class:`Set` or collection of type specified in *type* argument

        .. note::
            The same behavior as at :func:`difference` applies.

        .. warning::
            **Operation is not atomic.**
        """
        return self._operation('union', 'suninon', 'sunionstore',
                               others, type=kwargs.get('type'))

    def __or__(self, other):
        """Return a new set with elements from the set and the *other*.

        :param other: Set object (instance of :class:`collections.Set`
                      ABC, so built-in sets and frozensets are also accepted),
                      otherwise :exc:`TypeError` is raised.
        :rtype: type of the first operand

        .. note::
            The same behavior as at :func:`__sub__` applies.

        .. warning::
            **Operation is not atomic.**
        """
        return self._binary_operation('or_', 'sunionstore', other)

    def __ror__(self, other):
        return self._binary_operation('or_', 'sunionstore', other, right=True)

    def _update(self, data, others=None, pipe=None):
        redis = pipe or self.redis
        others = [data] + list(others or [])

        if self._are_redis_sets(others):
            # operation can be performed in Redis completely
            keys = [other.key for other in others]
            redis.sunionstore(self.key, self.key, *keys)
        else:
            elements = map(self._pickle, frozenset(itertools.chain(*others)))
            redis.sadd(self.key, *elements)

    def update(self, *others):
        """Update the set, adding elements from all *others*.

        :param others: Iterables, each one as a single positional argument.
        :rtype: None

        .. note::
            A bit different behavior takes place in comparing
            with the one described at :func:`difference_update`. Operation
            is **always performed in Redis**, regardless the types given.
            If *others* are instances of :class:`Set`, the performance
            should be better as no transfer of data is necessary at all.

        .. warning::
            **Operation is not atomic.**
        """
        self._update(others[0], others=others[1:])

    def __ior__(self, other):
        """Update the set, adding elements from the *other*.

        :param other: Set object (instance of :class:`collections.Set`
                      ABC, so built-in sets and frozensets are also accepted),
                      otherwise :exc:`TypeError` is raised.
        :rtype: None

        .. note::
            The same behavior as at :func:`__isub__` applies.

        .. warning::
            **Operation is not atomic.**
        """
        return self._binary_operation('or_', 'sinterstore', other,
                                      update=True)

    def symmetric_difference(self, other, **kwargs):
        """Return a new set with elements in either the set or *other* but not
        both.

        :param others: Any kind of iterable.
        :param type: Keyword argument, type of result, defaults to the same
                     type as collection (:class:`Set`, if not inherited).
        :rtype: :class:`Set` or collection of type specified in *type* argument

        .. note::
            The same behavior as at :func:`difference` applies.

        .. warning::
            **Operation is not atomic.**
        """
        return_type = kwargs.get('type') or Set

        if isinstance(other, Set):
            # operation can be performed partly in Redis and returned to Python
            with self.redis.pipeline() as pipe:
                pipe.sdiff(self.key, other.key)
                pipe.sdiff(other.key, self.key)
                diff1, diff2 = pipe.execute()
            elements = map(self._unpickle, diff1 | diff2)
            return self._create_new(elements, type=return_type)

        # else do it in Python completely,
        # simulating the same operation on standard set
        elements = set(self).symmetric_difference(other)
        return self._create_new(elements, type=return_type)

    def __xor__(self, other):
        """Update the set, keeping only elements found in either set, but not
        in both.

        :param other: Set object (instance of :class:`collections.Set`
                      ABC, so built-in sets and frozensets are also accepted),
                      otherwise :exc:`TypeError` is raised.
        :rtype: type of the first operand

        .. note::
            The same behavior as at :func:`__sub__` applies.

        .. warning::
            **Operation is not atomic.**
        """
        if not isinstance(other, collections.Set):  # collections.Set is ABC
            raise TypeError('Only sets are supported as operand types.')
        return self.symmetric_difference(other)

    def __rxor__(self, other):
        return self.__xor__(other)  # commutative

    def symmetric_difference_update(self, other):
        """Update the set, keeping only elements found in either set, but not
        in both.

        :param others: Any kind of iterable.
        :rtype: None

        .. note::
            A bit different behavior takes place in comparing
            with the one described at :func:`difference_update`. Operation
            is **always performed in Redis**, regardless the types given.
            If *others* are instances of :class:`Set`, the performance
            should be better as no transfer of data is necessary at all.

        .. warning::
            **Operation is not atomic.**
        """
        if isinstance(other, Set):
            # operation can be performed in Redis completely
            with self.redis.pipeline() as pipe:
                pipe.sdiff(self.key, other.key)
                pipe.sdiff(other.key, self.key)
                diff1, diff2 = pipe.execute()
            elements = diff1 | diff2
            return self._create_new(elements, id=self.id)

        # else do it in Python completely,
        # simulating the same operation on standard set
        elements = frozenset(self) ^ frozenset(other)
        return self._create_new(elements, id=self.id)

    def __ixor__(self, other):
        """Update the set, keeping only elements found in either set, but not
        in both.

        :param other: Set object (instance of :class:`collections.Set`
                      ABC, so built-in sets and frozensets are also accepted),
                      otherwise :exc:`TypeError` is raised.
        :rtype: None

        .. note::
            The same behavior as at :func:`__isub__` applies.

        .. warning::
            **Operation is not atomic.**
        """
        if not isinstance(other, collections.Set):  # collections.Set is ABC
            raise TypeError('Only sets are supported as operand types.')
        return self.symmetric_difference_update(other)

    def __eq__(self, other):
        if not isinstance(other, collections.Set):
            return NotImplemented
        if isinstance(other, Set):
            with self.redis.pipeline() as pipe:
                pipe.smembers(self.key)
                pipe.smembers(other.key)
                members1, members2 = pipe.execute()
            return members1 == members2
        return frozenset(self) == frozenset(other)

    def __le__(self, other):
        if not isinstance(other, collections.Set):
            return NotImplemented
        return self.issubset(other)

    def __lt__(self, other):
        if not isinstance(other, collections.Set):
            return NotImplemented
        if isinstance(other, Set):
            with self.redis.pipeline() as pipe:
                pipe.smembers(self.key)
                pipe.sinter(self.key, other.key)
                pipe.scard(other.key)
                members, inters, other_size = pipe.execute()
            return (members == inters and len(members) != other_size)
        return frozenset(self) < frozenset(other)

    def issubset(self, other):
        """Test whether every element in the set is in other.

        :param other: Any kind of iterable.
        :rtype: boolean
        """
        if isinstance(other, Set):
            with self.redis.pipeline() as pipe:
                pipe.smembers(self.key)
                pipe.sinter(self.key, other.key)
                members, inters = pipe.execute()
            return members == inters
        return frozenset(self) <= frozenset(other)

    def issuperset(self, other):
        if isinstance(other, collections.Set):
            return other <= self
        else:
            return frozenset(other) <= self


class SortedSet(RedisCollection, collections.MutableSet):
    """Mutable **sorted set** collection aiming to have the same API as the
    standard set type. See `set
    <http://docs.python.org/2/library/stdtypes.html#set>`_ for
    further details. The Redis implementation is based on the
    `sorted set <http://redis.io/commands#sorted_set>`_ type.
    """

    # http://code.activestate.com/recipes/576694/

    def __init__(self):
        pass

    def __len__(self):
        pass

    def __iter__(self):
        pass

    def __contains__(self, elem):
        pass

    def add(self, elem):
        pass

    def discard(self, elem):
        pass
