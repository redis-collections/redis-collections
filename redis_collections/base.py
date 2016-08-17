# -*- coding: utf-8 -*-
"""
base
~~~~
"""
from __future__ import division, print_function, unicode_literals

import abc
from decimal import Decimal
from fractions import Fraction
import uuid

try:
    import cPickle as pickle
except ImportError:
    import pickle as pickle  # NOQA
import functools

import redis
import six

NUMERIC_TYPES = six.integer_types + (float, Decimal, Fraction, complex)


def same_types(fn):
    """Decorator, helps to check whether operands are of
    the same type as *self*. It is possible to extend
    allowed types by defining ``_same_types`` class property
    with tuple of allowed classes.
    """
    @functools.wraps(fn)
    def wrapper(self, *args):
        types = (self.__class__,) + self._same_types

        # all args must be an instance of any of the types
        allowed = all([
            any([isinstance(arg, t) for t in types])
            for arg in args
        ])

        if not allowed:
            types_msg = ', '.join(types[:-1])
            types_msg = ' or '.join([types_msg, types[-1]])
            message = ('Only instances of %s are '
                       'supported as operand types.') % types_msg
            raise TypeError(message)

        return fn(self, *args)
    return wrapper


@six.add_metaclass(abc.ABCMeta)
class RedisCollection(object):
    """Abstract class providing backend functionality for all the other
    Redis collections.
    """

    _same_types = ()

    not_impl_msg = ('Cannot be implemented efficiently or atomically '
                    'due to limitations in Redis command set.')

    @abc.abstractmethod
    def __init__(self, data=None, redis=None, key=None):
        """
        :param data: Initial data.
        :param redis: Redis client instance. If not provided, default Redis
                      connection is used.
        :type redis: :class:`redis.StrictRedis`
        :param key: Redis key of the collection. Collections with the same key
                    point to the same data. If not provided, default random
                    string is generated.
        :type key: str

        .. note::
            :func:`uuid.uuid4` is used for default key generation.
            If you are not satisfied with its `collision
            probability <http://stackoverflow.com/a/786541/325365>`_,
            make your own implementation by subclassing and overriding
            internal method :func:`_create_key`.
        """
        #: Redis client instance. :class:`StrictRedis` object with default
        #: connection settings is used if not set by :func:`__init__`.
        self.redis = redis or self._create_redis()

        #: Redis key of the collection.
        self.key = key or self._create_key()

        # data initialization
        if data is not None:
            if isinstance(data, RedisCollection):
                # wrap into transaction
                def init_trans(pipe):
                    d = data._data(pipe=pipe)  # retrieve
                    pipe.multi()
                    self._init_data(d, pipe=pipe)  # store
                self._transaction(init_trans)
            else:
                self._init_data(data)

    def _create_new(self, data=None, key=None, pipe=None, cls=None):
        """Shorthand for creating instances of any collections. *cls*
        specifies the type of collection to be created. If subclass of
        :class:`RedisCollection` given, all settings from current ``self``
        are propagated.

        :param data: Initial data in form of a classic, built-in collection.
        :param key: Redis key of the instance. Ignored if *cls* is not a
                   :class:`RedisCollection` subclass.
        :type key: string
        :param pipe: Redis pipe in case creation is performed as a part
                     of transaction. Ignored if *cls* is not a
                     :class:`RedisCollection` subclass.
        :type pipe: :class:`redis.client.StrictPipeline` or
                    :class:`redis.client.StrictRedis`
        :param cls: Type of the collection. Defaults to ``self.__class__``.
        :type cls: Class object.
        """
        assert not isinstance(data, RedisCollection), \
            "Not atomic. Use '_data()' within a transaction first."
        cls = cls or self.__class__
        if issubclass(cls, RedisCollection):
            settings = {
                'key': key,
                'redis': self.redis,
            }

            if pipe is not None and data:
                # here we cannot use cls(data, **settings), because
                # that would not be atomic within the transaction
                new = cls(**settings)
                new._init_data(data, pipe=pipe)
                return new
            return cls(data, **settings)

        return cls(data) if data else cls()

    def _init_data(self, data, pipe=None):
        """Data initialization helper.

        :param data: Initial data in form of a classic, built-in collection.
        :param pipe: Redis pipe in case creation is performed as a part
                     of transaction.
        :type pipe: :class:`redis.client.StrictPipeline` or
                    :class:`redis.client.StrictRedis`
        """
        assert not isinstance(data, RedisCollection), \
            "Not atomic. Use '_data()' within a transaction first."

        # if not in pipe, make your own
        p = pipe if pipe is not None else self.redis.pipeline()
        if data is not None:
            self._clear(pipe=p)
            if data:
                # non-empty data, populate collection
                self._update(data, pipe=p)
        if pipe is None:
            # own pipe, execute it
            p.execute()

    def _create_redis(self):
        """Creates default Redis connection.

        :rtype: :class:`redis.StrictRedis`
        """
        return redis.StrictRedis()

    def _create_key(self):
        """Creates new Redis key.

        :rtype: string

        .. note::
            :func:`uuid.uuid4` is used. If you are not satisfied with its
            `collision
            probability <http://stackoverflow.com/a/786541/325365>`_,
            make your own implementation by subclassing and overriding this
            method.
        """
        return uuid.uuid4().hex

    @abc.abstractmethod
    def _data(self, pipe=None):
        """Helper for getting collection's data within a transaction.

        :param pipe: Redis pipe in case creation is performed as a part
                     of transaction.
        :type pipe: :class:`redis.client.StrictPipeline` or
                    :class:`redis.client.StrictRedis`
        """
        pass

    def _pickle(self, data):
        """Converts given data to string.

        :param data: Data to be serialized.
        :type data: anything serializable
        :rtype: string
        """
        return pickle.dumps(data)

    def _unpickle(self, string):
        """Converts given string serialization back to corresponding data.
        If :obj:`None` or empty string given, :obj:`None` is returned.

        :param string: String to be unserialized.
        :type string: string
        :rtype: anything serializable
        """
        return pickle.loads(string) if string else None

    def _update(self, data, pipe=None):
        """Helper for update operations.

        :param data: Data for update in form of a classic, built-in collection.
        :param pipe: Redis pipe in case update is performed as a part
                     of transaction.
        :type pipe: :class:`redis.client.StrictPipeline` or
                    :class:`redis.client.StrictRedis`
        """
        assert not isinstance(data, RedisCollection), \
            "Not atomic. Use '_data()' within a transaction first."

    def _clear(self, pipe=None):
        """Helper for clear operations.

        :param pipe: Redis pipe in case update is performed as a part
                     of transaction.
        :type pipe: :class:`redis.client.StrictPipeline` or
                    :class:`redis.client.StrictRedis`
        """
        redis = pipe if pipe is not None else self.redis
        redis.delete(self.key)

    def clear(self):
        """Completely cleares the collection's data."""
        self._clear()

    def _transaction(self, fn, *extra_keys):
        """Helper simplifying code within watched transaction.

        Takes *fn*, function treated as a transaction. Returns whatever
        *fn* returns. ``self.key`` is watched. *fn* takes *pipe* as the
        only argument.

        :param fn: Closure treated as a transaction.
        :type fn: function *fn(pipe)*
        :param extra_keys: Optional list of additional keys to watch.
        :type extra_keys: list
        :rtype: whatever *fn* returns
        """
        results = []

        def trans(pipe):
            results.append(fn(pipe))

        self.redis.transaction(trans, self.key, *extra_keys)
        return results[0]

    def copy(self, key=None):
        """Return a copy of the collection.

        :param key: Key of the new collection. Defaults to auto-generated.
        :type key: string
        """
        def copy_trans(pipe):
            data = self._data(pipe=pipe)  # retrieve
            pipe.multi()
            return self._create_new(data, key=key, pipe=pipe)  # store
        return self._transaction(copy_trans, key)

    def _repr_data(self, data):
        return repr(data)

    def __repr__(self):
        cls_name = self.__class__.__name__
        data = self._repr_data(self._data())
        return '<redis_collections.%s at %s %s>' % (cls_name, self.key, data)
