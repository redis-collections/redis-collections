# -*- coding: utf-8 -*-
"""
sortedsets
~~~~~

Collections based on the Redis Sorted Sets data type
"""
from __future__ import division, print_function, unicode_literals

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

    def _repr_data(self):
        items = ('{}: {}'.format(repr(k), repr(v)) for k, v in self.items())
        return '{{{}}}'.format(', '.join(items))

    # Magic methods

    def __contains__(self, member, pipe=None):
        pipe = self.redis if pipe is None else pipe
        score = pipe.zscore(self.key, self._pickle(member))

        return score is not None

    def __iter__(self, pipe=None):
        pipe = self.redis if pipe is None else pipe

        return iter(self._data(pipe))

    def __len__(self, pipe=None):
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
        pipe = self.redis if pipe is None else pipe
        pipe.zrem(self.key, self._pickle(member))

    def get_score(self, member, default=None, pipe=None):
        pipe = self.redis if pipe is None else pipe
        score = pipe.zscore(self.key, self._pickle(member))

        if (score is None) and (default is not None):
            score = float(default)

        return score

    def get_rank(self, member, reverse=False, pipe=None):
        pipe = self.redis if pipe is None else pipe
        method = getattr(pipe, 'zrevrank' if reverse else 'zrank')
        rank = method(self.key, self._pickle(member))

        return rank

    def increment_score(self, member, amount=1):
        self.redis.zincrby(self.key, self._pickle(member), float(amount))

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

    def set_score(self, member, score, pipe=None):
        pipe = self.redis if pipe is None else pipe
        pipe.zadd(self.key, float(score), self._pickle(member))

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
