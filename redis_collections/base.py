# -*- coding: utf-8 -*-
"""
base
~~~~
"""


import uuid
import redis
import functools
from abc import ABCMeta, abstractmethod

try:
    import cPickle as pickle
except ImportError:
    import pickle as pickle


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


class RedisCollection:
    """Abstract class providing backend functionality for all the other
    Redis collections.
    """

    __metaclass__ = ABCMeta

    _same_types = ()

    not_impl_msg = ('Cannot be implemented efficiently or atomically '
                    'due to limitations in Redis command set.')

    @abstractmethod
    def __init__(self, data=None, redis=None, key=None, pickler=None,
                 prefix=None):
        """
        :param data: Initial data.
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
        :param prefix: Key prefix to use when working with Redis. Defaults
                       to empty string.
        :type prefix: str

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

        #: Class or module implementing pickling. Standard :mod:`pickle`
        #: module is set as default.
        self.pickler = pickler or pickle

        #: Redis key prefix. Defaults to an empty string.
        self.prefix = prefix

        #: Redis key of the collection.
        self.key = self._create_key(key, prefix)

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
                'pickler': self.pickler,
                'prefix': self.prefix,
            }

            if pipe and data:
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

        p = pipe or self.redis.pipeline()  # if not in pipe, make your own
        if data is not None:
            self._clear(pipe=p)
            if data:
                # non-empty data, populate collection
                self._update(data, pipe=p)
        if not pipe:
            # own pipe, execute it
            p.execute()

    def _create_redis(self):
        """Creates default Redis connection.

        :rtype: :class:`redis.StrictRedis`
        """
        return redis.StrictRedis()

    def _create_key(self, key=None, prefix=None):
        """Creates new Redis key.

        :param prefix: Key. If :obj:`None`, random string is generated.
        :type prefix: string
        :param prefix: Key prefix.
        :type prefix: string
        :rtype: string

        .. note::
            :func:`uuid.uuid4` is used. If you are not satisfied with its
            `collision
            probability <http://stackoverflow.com/a/786541/325365>`_,
            make your own implementation by subclassing and overriding this
            method.
        """
        components = [
            prefix or '',
            key or uuid.uuid4().hex,
        ]
        return ''.join(components)

    @abstractmethod
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
        return unicode(self.pickler.dumps(data))

    def _unpickle(self, string):
        """Converts given string serialization back to corresponding data.
        If :obj:`None` or empty string given, :obj:`None` is returned.

        :param string: String to be unserialized.
        :type string: string
        :rtype: anything serializable
        """
        if string is None:
            return None
        if not isinstance(string, basestring):
            msg = 'Only strings can be unpickled (%r given).' % string
            raise TypeError(msg)
        return self.pickler.loads(string)

    @abstractmethod
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
        redis = pipe or self.redis
        redis.delete(self.key)

    def clear(self):
        """Completely cleares the collection's data."""
        self._clear()

    def _transaction(self, fn, extra_keys=None):
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
        extra_keys = extra_keys or []

        def trans(pipe):
            results.append(fn(pipe))

        self.redis.transaction(trans, self.key, *extra_keys)
        return results[0]

    def _transaction_with_new(self, fn, new_key=None, extra_keys=None):
        """Helper simplifying code within transaction which
        creates a new instance of a Redis collection.

        Takes *fn*, function treated as a transaction. Returns whatever
        *fn* returns. ``self.key`` and the new key are watched.
        *fn* takes *pipe* as the first argument and the new key as the second.

        If *new_key* given, it is used instead of a newly generated one.

        :param fn: Closure treated as a transaction.
        :type fn: function *fn(pipe, new_key)*
        :param new_key: Unprefixed key to be used for new instance creation.
        :type new_key: string
        :param extra_keys: Optional list of additional keys to watch.
        :type extra_keys: list
        :rtype: whatever *fn* returns
        """
        results = []
        extra_keys = extra_keys or []

        def trans(pipe):
            results.append(fn(pipe, new_key))

        self.redis.transaction(trans, self.key, new_key, *extra_keys)
        return results[0]

    def copy(self, key=None):
        """Return a copy of the collection.

        :param key: Unprefixed key of the new collection.
                    Defaults to auto-generated.
        :type key: string
        """
        def copy_trans(pipe, new_key):
            data = self._data(pipe=pipe)  # retrieve
            pipe.multi()
            return self._create_new(data, key=new_key, pipe=pipe)  # store
        return self._transaction_with_new(copy_trans, new_key=key)

    def _repr_data(self, data):
        return repr(data)

    def __repr__(self):
        cls_name = self.__class__.__name__
        data = self._repr_data(self._data())
        return '<redis_collections.%s at %s %s>' % (cls_name, self.key, data)
