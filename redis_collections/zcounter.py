# -*- coding: utf-8 -*-
"""
dicts
~~~~~

Collections based on the dict interface.
"""
from __future__ import division, print_function, unicode_literals

import six

from .base import RedisCollection


class ZCounter(RedisCollection):
    if six.PY2:
        _pickle = RedisCollection._pickle_2
        _unpickle = RedisCollection._unpickle_2
    else:
        _pickle = RedisCollection._pickle_3

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

    def __delitem__(self, member):
        # zrem
        pass

    def _get_slice(self, index):
        raise NotImplementedError()

    def __getitem__(self, member, pipe=None):
        if isindex(member, slice):
            return self._get_slice(member)

        pipe = self.redis if pipe is None else pipe
        score = pipe.zscore(self.key, self._pickle(member))

        if score is None:
            raise KeyError('{} is not in the collection'.format(member))

        return score

    def __iter__(self, pipe=None):
        pipe = self.redis if pipe is None else pipe

        return iter(self._data(pipe))

    def __len__(self, pipe=None):
        pipe = self.redis if pipe is None else pipe
        score = pipe.zcard(self.key)

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

    def count(self, start=None, stop=None):
        pass

    def index(self, member):
        pass

    def items(self, start=None, stop=None, reverse=False):
        pass

    def update(self, other):
        pass
