# -*- coding: utf-8 -*-
"""
RedisCollectionsFactory
@author: Mardix - https://github.com/mardix
"""

import redis
from . import (
    Counter,
    DefaultDict,
    Deque,
    Dict,
    List,
    Set,
    SortedSetCounter,
    LRUDict,
    SyncableDict,
    SyncableCounter,
    SyncableDeque,
    SyncableDefaultDict,
    SyncableList,
    SyncableSet,
)


class RedisCollectionsFactory(redis.Redis):
    """
    This class factory exposes the redis-py methods
    along with the redis collections
    At a minimum the collections type require a key name, and it will use
    the current collection as redis connection.

    Examples:

    my_redis = RedisCollectionsFactory.from_url("redis://x:x@host.com/dbname")

    my_dict = my_redis.Dict(key, {"a": "b"}, ...)
    my_list = my_redis.List(key, ["A", "B", "C"]...)

    you still can use redis native:

    my_redis.set(key, value)
    my_redis.get(key)

    """

    def Counter(self, key, *args, **kwargs):
        kwargs.update({"key": key, "redis": self})
        return Counter(*args, **kwargs)

    def DefaultDict(self, key, *args, **kwargs):
        kwargs.update({"key": key, "redis": self})
        return DefaultDict(*args, **kwargs)

    def Deque(self, key, **kwargs):
        kwargs.update({"key": key, "redis": self})
        return Deque(**kwargs)

    def Dict(self, key, *args, **kwargs):
        kwargs.update({"key": key, "redis": self})
        return Dict(*args, **kwargs)

    def List(self, key, *args, **kwargs):
        kwargs.update({"key": key, "redis": self})
        return List(*args, **kwargs)

    def LRUDict(self, key, **kwargs):
        kwargs.update({"key": key, "redis": self})
        return LRUDict(**kwargs)

    def Set(self, key, *args, **kwargs):
        kwargs.update({"key": key, "redis": self})
        return Set(*args, **kwargs)

    def SortedSetCounter(self, key, *args, **kwargs):
        kwargs.update({"key": key, "redis": self})
        return SortedSetCounter(*args, **kwargs)

    def SyncableDict(self, key, **kwargs):
        kwargs.update({"key": key, "redis": self})
        return SyncableDict(**kwargs)

    def SyncableCounter(self, key, **kwargs):
        kwargs.update({"key": key, "redis": self})
        return SyncableCounter(**kwargs)

    def SyncableDefaultDict(self, key, *args, **kwargs):
        kwargs.update({"key": key, "redis": self})
        return SyncableDefaultDict(*args, **kwargs)

    def SyncableDeque(self, key, *args, **kwargs):
        kwargs.update({"key": key, "redis": self})
        return SyncableDeque(*args, **kwargs)

    def SyncableList(self, key, **kwargs):
        kwargs.update({"key": key, "redis": self})
        return SyncableList(**kwargs)

    def SyncableSet(self, key, **kwargs):
        kwargs.update({"key": key, "redis": self})
        return SyncableSet(**kwargs)
