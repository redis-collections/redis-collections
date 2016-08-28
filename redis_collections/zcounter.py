# -*- coding: utf-8 -*-
"""
sortedsets
~~~~~

Collections based on the Redis Sorted Sets data type
"""
from __future__ import division, print_function, unicode_literals

import itertools

import six

from .base import RedisCollection


class ZCounter(RedisCollection):
    def __init__(self, *args, **kwargs):
        data = args[0] if args else kwargs.pop('data', None)

        super(ZCounter, self).__init__(**kwargs)

        if data:
            self.update(data)

    def _data(self, pipe=None):
        pipe = self.redis if pipe is None else pipe
        items = pipe.zrange(self.key, 0, -1, withscores=True)

        return [(self._unpickle(member), score) for member, score in items]

    # Magic methods

    def __contains__(self, member, pipe=None):
        pipe = self.redis if pipe is None else pipe
        score = pipe.zscore(self.key, self._pickle(member))

        return score is not None

    def _del_slice(self, index):
        def del_slice_trans(pipe):
            start, stop, step, forward, len_self = self._normalize_slice(
                index, pipe
            )

            if start == stop:
                return

            # For slices with steps we find the members to remove by index,
            # then remove them.
            if index.step is not None:
                all_items = pipe.zrangebyscore(
                    self.key, float('-inf'), float('inf')
                )
                pipe.multi()
                for i in list(six.moves.xrange(len_self))[index]:
                    pickled_member = all_items[i]
                    pipe.zrem(self.key, pickled_member)
            # Otherwise ranks should match Python indexes
            else:
                pipe.zremrangebyrank(self.key, start, max(stop - 1, 0))

        return self._transaction(del_slice_trans)

    def __delitem__(self, member, pipe=None):
        if isinstance(member, slice):
            return self._del_slice(member)

        pipe = self.redis if pipe is None else pipe
        deleted_count = pipe.zrem(self.key, self._pickle(member))

        if deleted_count == 0:
            raise KeyError

    def _get_slice(self, index):
        def get_slice_trans(pipe):
            start, stop, step, forward, len_self = self._normalize_slice(
                index, pipe
            )

            if start == stop:
                return []

            ret = []
            redis_values = pipe.zrange(
                self.key, start, max(stop - 1, 0), withscores=True
            )
            for pickled_member, score in redis_values:
                member = self._unpickle(pickled_member)
                ret.append((member, score))

            if not forward:
                ret = reversed(ret)

            if step != 1:
                ret = itertools.islice(ret, None, None, step)

            return list(ret)

        return self._transaction(get_slice_trans)

    def __getitem__(self, member, pipe=None):
        if isinstance(member, slice):
            return self._get_slice(member)

        pipe = self.redis if pipe is None else pipe
        score = pipe.zscore(self.key, self._pickle(member))

        if score is None:
            raise KeyError

        return score

    def __iter__(self, pipe=None):
        pipe = self.redis if pipe is None else pipe

        return iter(self._data(pipe))

    def __len__(self, pipe=None):
        pipe = self.redis if pipe is None else pipe

        return pipe.zcard(self.key)

    def __setitem__(self, member, score, pipe=None):
        pipe = self.redis if pipe is None else pipe
        pipe.zadd(self.key, float(score), self._pickle(member))

    # Named methods

    def clear(self, pipe=None):
        self._clear(pipe=pipe)

    def copy(self, key=None):
        other = self.__class__(redis=self.redis, key=key)
        other.update(self)

        return other

    def count_between(self, min_score=None, max_score=None):
        min_score = float('-inf') if min_score is None else float(min_score)
        max_score = float('inf') if max_score is None else float(max_score)

        return self.redis.zcount(self.key, min_score, max_score)

    def get(self, member, default=None):
        try:
            score = self.__getitem__(member)
        except KeyError:
            if default is not None:
                score = default
            else:
                raise

        return score

    def increment(self, member, amount=1):
        self.redis.zincrby(self.key, self._pickle(member), float(amount))

    def index(self, member, reverse=False, pipe=None):
        pipe = self.redis if pipe is None else pipe
        method = getattr(pipe, 'zrevrank' if reverse else 'zrank')
        rank = method(self.key, self._pickle(member))

        if rank is None:
            raise KeyError

        return rank

    def items_by_rank(
        self, min_rank=None, max_rank=None, reverse=False, pipe=None
    ):
        pipe = self.redis if pipe is None else pipe

        min_rank = 0 if min_rank is None else min_rank
        max_rank = -1 if max_rank is None else max_rank

        if reverse:
            results = pipe.zrevrange(
                self.key, max_rank, min_rank, withscores=True
            )
        else:
            results = pipe.zrange(
                self.key, min_rank, max_rank, withscores=True
            )

        return [(self._unpickle(member), score) for member, score in results]

    def items_by_score(self, min_score, max_score, reverse=False, pipe=None):
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
        pipe = self.redis if pipe is None else pipe

        no_ranks = (min_rank is None) and (max_rank is None)
        no_scores = (min_score is None) and (max_score is None)

        if no_ranks and no_scores:
            ret = self.items_by_score(min_score, max_score, reverse, pipe)
        elif no_ranks and (not no_scores):
            ret = self.items_by_score(min_score, max_score, reverse, pipe)
        elif (not no_ranks) and no_scores:
            ret = self.items_by_rank(min_rank, max_rank, reverse, pipe)
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

    def update(self, other):
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
