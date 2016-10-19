# -*- coding: utf-8 -*-
"""
sortedsets
~~~~~~~~~~

The `sortedsets` module contains a collection, :class:`SortedSetCounter`,
which provides an interface to Redis's
`Sorted Set <http://redis.io/commands#set>`_ type.
"""
from __future__ import division, print_function, unicode_literals

from .base import RedisCollection


class SortedSetCounter(RedisCollection):
    """
    :class:`SortedSetCounter` is a collection based on the Redis
    `Sorted Set <http://redis.io/topics/data-types#sorted-sets>`_ type.
    Instances map a unique set of ``member`` objects to floating point
    ``score`` values.

        >>> ssc = SortedSetCounter([('earth', 300), ('mercury', 100)])
        >>> ssc.set_score('venus', 200)
        >>> ssc.get_score('venus')
        200.0

    When retrieving members they are returned in order by score:

        >>> ssc.items()
        [('mercury', 100.0), ('venus', 200.0), ('earth', 300.0)]

    Ranges of items by rank can be computed and returned efficiently, as can
    ranges by score:

        >>> ssc.items(min_rank=200.0)
        [('venus', 200.0), ('earth', 300.0)]
        >>> ssc.items(min_score=99, max_score=299)
        [('mercury', 100.0), ('venus', 200.0)]

    .. warning::
        The API for :class:`SortedSetCounter` does not attempt to match an
        existing Python collection's.

        - Unlike :class:`Dict` or :class:`Set` objects, equal numeric types are
          considered distinct when used as members. For example, a collection
          can contain both ``1`` and ``1.0``.

        - Unlike :class:`Counter` or :class:`collections.Counter` objects, only
          :class:`float` scores can be stored.
    """

    def __init__(self, *args, **kwargs):
        """
        Create a new SortedSetCounter object.

        If the first argument (*data*) is an iterable object, create the new
        SortedSetCounter with its elements as the initial data.

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
        data = args[0] if args else kwargs.pop('data', None)

        super(SortedSetCounter, self).__init__(**kwargs)

        if data:
            self.update(data)

    def _data(self, pipe=None):
        pipe = self.redis if pipe is None else pipe
        items = pipe.zrange(self.key, 0, -1, withscores=True)

        return [(self._unpickle(member), score) for member, score in items]

    def _repr_data(self):
        items = ('{}: {}'.format(repr(k), repr(v)) for k, v in self.items())
        return '{{{}}}'.format(', '.join(items))

    # Magic methods

    def __contains__(self, member, pipe=None):
        """Return ``True`` if *member* is present, else ``False``."""
        pipe = self.redis if pipe is None else pipe
        score = pipe.zscore(self.key, self._pickle(member))

        return score is not None

    def __iter__(self, pipe=None):
        """
        Return an iterator of ``(member, score)`` tuples from the collection.
        """
        pipe = self.redis if pipe is None else pipe

        return iter(self._data(pipe))

    def __len__(self, pipe=None):
        """Return the number of members in the collection."""
        pipe = self.redis if pipe is None else pipe

        return pipe.zcard(self.key)

    # Named methods

    def clear(self, pipe=None):
        self._clear(pipe=pipe)

    def copy(self, key=None):
        other = self.__class__(redis=self.redis, key=key)
        other.update(self)

        return other

    def count_between(self, min_score=None, max_score=None):
        """
        Returns the number of members whose score is between *min_score* and
        *max_score* (inclusive).
        """
        min_score = float('-inf') if min_score is None else float(min_score)
        max_score = float('inf') if max_score is None else float(max_score)

        return self.redis.zcount(self.key, min_score, max_score)

    def discard_by_rank(self, min_rank=None, max_rank=None, pipe=None):
        pipe = self.redis if pipe is None else pipe

        min_rank = 0 if min_rank is None else min_rank
        max_rank = -1 if max_rank is None else max_rank

        pipe.zremrangebyrank(self.key, min_rank, max_rank)

    def discard_by_score(self, min_score=None, max_score=None, pipe=None):
        pipe = self.redis if pipe is None else pipe

        min_score = float('-inf') if min_score is None else float(min_score)
        max_score = float('inf') if max_score is None else float(max_score)

        pipe.zremrangebyscore(self.key, min_score, max_score)

    def discard_between(
        self,
        min_rank=None,
        max_rank=None,
        min_score=None,
        max_score=None,
    ):
        """
        Remove members whose ranking is between *min_rank* and *max_rank*
        OR whose score is between *min_score* and *max_score* (both ranges
        inclusive). If no bounds are specified, no members will be removed.
        """
        no_ranks = (min_rank is None) and (max_rank is None)
        no_scores = (min_score is None) and (max_score is None)

        # Default scope: nothing
        if no_ranks and no_scores:
            return

        # Scope widens to given score range
        if no_ranks and (not no_scores):
            return self.discard_by_score(min_score, max_score)

        # Scope widens to given rank range
        if (not no_ranks) and no_scores:
            return self.discard_by_rank(min_rank, max_rank)

        # Scope widens to score range and then rank range
        with self.redis.pipeline() as pipe:
            self.discard_by_score(min_score, max_score, pipe)
            self.discard_by_rank(min_rank, max_rank, pipe)
            pipe.execute()

    def discard_member(self, member, pipe=None):
        """
        Remove *member* from the collection, unconditionally.
        """
        pipe = self.redis if pipe is None else pipe
        pipe.zrem(self.key, self._pickle(member))

    def get_score(self, member, default=None, pipe=None):
        """
        Return the score of *member*, or *default* if it is not in the
        collection.
        """
        pipe = self.redis if pipe is None else pipe
        score = pipe.zscore(self.key, self._pickle(member))

        if (score is None) and (default is not None):
            score = float(default)

        return score

    def get_or_set_score(self, member, default=0):
        """
        If *member* is in the collection, return its value. If not, store it
        with a score of *default* and return *default*. *default* defaults to
        0.
        """
        default = float(default)

        def get_or_set_score_trans(pipe):
            pickled_member = self._pickle(member)
            score = pipe.zscore(self.key, pickled_member)

            if score is None:
                pipe.zadd(self.key, default, self._pickle(member))
                return default

            return score

        return self._transaction(get_or_set_score_trans)

    def get_rank(self, member, reverse=False, pipe=None):
        """
        Return the rank of *member* in the collection.
        By default, the member with the lowest score has rank 0.
        If *reverse* is ``True``, the member with the highest score has rank 0.
        """
        pipe = self.redis if pipe is None else pipe
        method = getattr(pipe, 'zrevrank' if reverse else 'zrank')
        rank = method(self.key, self._pickle(member))

        return rank

    def increment_score(self, member, amount=1):
        """
        Adjust the score of *member* by *amount*. If *member* is not in the
        collection it will be stored with a score of *amount*.
        """
        return self.redis.zincrby(
            self.key, self._pickle(member), float(amount)
        )

    def items_by_rank(
        self, min_rank=None, max_rank=None, reverse=False, pipe=None
    ):
        pipe = self.redis if pipe is None else pipe

        min_rank = 0 if min_rank is None else min_rank
        max_rank = -1 if max_rank is None else max_rank

        if reverse:
            results = pipe.zrevrange(
                self.key, min_rank, max_rank, withscores=True
            )
        else:
            results = pipe.zrange(
                self.key, min_rank, max_rank, withscores=True
            )

        return [(self._unpickle(member), score) for member, score in results]

    def items_by_score(
        self, min_score=None, max_score=None, reverse=False, pipe=None
    ):
        pipe = self.redis if pipe is None else pipe

        min_score = float('-inf') if min_score is None else float(min_score)
        max_score = float('inf') if max_score is None else float(max_score)

        if reverse:
            results = pipe.zrevrangebyscore(
                self.key, max_score, min_score, withscores=True
            )
        else:
            results = pipe.zrangebyscore(
                self.key, min_score, max_score, withscores=True
            )

        return [(self._unpickle(member), score) for member, score in results]

    def items(
        self,
        min_rank=None,
        max_rank=None,
        min_score=None,
        max_score=None,
        reverse=False,
        pipe=None,
    ):
        """
        Return a list of ``(member, score)`` tuples whose ranking is between
        *min_rank* and *max_rank* AND whose score is between *min_score* and
        *max_score* (both ranges inclusive). If no bounds are specified, all
        items will be returned.
        """
        pipe = self.redis if pipe is None else pipe

        no_ranks = (min_rank is None) and (max_rank is None)
        no_scores = (min_score is None) and (max_score is None)

        # Default scope: everything
        if no_ranks and no_scores:
            ret = self.items_by_score(min_score, max_score, reverse, pipe)
        # Scope narrows to given score range
        elif no_ranks and (not no_scores):
            ret = self.items_by_score(min_score, max_score, reverse, pipe)
        # Scope narrows to given rank range
        elif (not no_ranks) and no_scores:
            ret = self.items_by_rank(min_rank, max_rank, reverse, pipe)
        # Scope narrows twice - once by rank and once by score
        else:
            results = self.items_by_rank(min_rank, max_rank, reverse, pipe)
            ret = []
            for member, score in results:
                if (min_score is not None) and (score < min_score):
                    continue
                if (max_score is not None) and (score > max_score):
                    continue
                ret.append((member, score))

        return ret

    def scan_items(self):
        """
        Yield each of the ``(member, score)`` tuples from the collection,
        without pulling them all into memory.

        .. warning::
            This method may return the same (member, score) tuple multiple
            times.
            See the `Redis SCAN documentation
            <http://redis.io/commands/scan#scan-guarantees>`_ for details.
        """
        for m, s in self.redis.zscan_iter(self.key):
            yield self._unpickle(m), s

    def set_score(self, member, score, pipe=None):
        """
        Set the score of *member* to *score*.
        """
        pipe = self.redis if pipe is None else pipe
        pipe.zadd(self.key, float(score), self._pickle(member))

    def update(self, other):
        """
        Update the collection with items from *other*. Accepts other
        :class:`SortedSetCounter` instances, dictionaries mapping members to
        numeric scores, or sequences of ``(member, score)`` tuples.
        """
        def update_trans(pipe):
            other_items = method(pipe=pipe) if use_redis else method()

            pipe.multi()
            for member, score in other_items:
                pipe.zadd(self.key, float(score), self._pickle(member))

        watches = []
        if self._same_redis(other, RedisCollection):
            use_redis = True
            watches.append(other.key)
        else:
            use_redis = False

        if hasattr(other, 'items'):
            method = other.items
        elif hasattr(other, '__iter__'):
            method = other.__iter__

        self._transaction(update_trans, *watches)
