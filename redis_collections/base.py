# -*- coding: utf-8 -*-
"""
base
~~~~
"""


import uuid
import redis
from abc import ABCMeta, abstractmethod

try:
    import cPickle as pickle
except ImportError:
    import pickle as pickle


class RedisCollection:
    """Abstract class providing backend functionality for all the other
    Redis collections.
    """

    __metaclass__ = ABCMeta

    not_impl_msg = ('Cannot be implemented efficiently or atomically '
                    'due to limitations in Redis command set.')

    @abstractmethod
    def __init__(self, data=None, redis=None, id=None, pickler=None,
                 prefix=None):
        """
        :param data: Initial data.
        :param redis: Redis client instance. If not provided, default Redis
                      connection is used.
        :type redis: :class:`redis.StrictRedis` or :obj:`None`
        :param id: ID of the collection. Collections with the same IDs point
                   to the same data. If not provided, default random ID string
                   is generated.
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
            make your own implementation by subclassing and overriding
            internal method :func:`_create_id`.
        """
        #: Redis client instance. :class:`StrictRedis` object with default
        #: connection settings is used if not set by :func:`__init__`.
        self.redis = redis or self._create_redis()

        #: Class or module implementing pickling. Standard :mod:`pickle`
        #: module is set as default.
        self.pickler = pickler or pickle

        #: Redis key prefix. Defaults to an empty string.
        self.prefix = prefix

        #: ID of the collection.
        self.id = id or self._create_id()
        self.key = self._create_key(self.id, prefix=prefix)

        # data initialization
        if data is not None:
            self._init_data(data)  # touches Redis

    @classmethod
    def fromanother(cls, obj, data=None, id=None, pipe=None):
        """Collection factory. Takes another object *obj* and
        creates collection with the same settings and data *data*.

        :param obj: Another :class:`RedisCollection` instance.
        :type obj: :class:`RedisCollection`
        :param data: Initial data.
        :param id: ID of the new collection.
        :type id: string
        :param pipe: Redis pipe in case creation is performed as a part
                     of transaction.
        :type pipe: :class:`redis.client.StrictPipeline` or
                    :class:`redis.client.StrictRedis`
        """
        assert isinstance(obj, RedisCollection)

        settings = {
            'id': id,
            'redis': obj.redis,
            'pickler': obj.pickler,
            'prefix': obj.prefix,
        }

        if pipe and data:
            # here we cannot use cls(data, **settings), because
            # that would not be atomic within the transaction
            new = cls(**settings)
            new._init_data(data, pipe=pipe)
            return new
        return cls(data, **settings)

    def _create_new(self, data=None, id=None, pipe=None, type=None):
        """Shorthand for creating instances of any collections. *type*
        specifies the type of collection to be created. If subclass of
        :class:`RedisCollection` given, all settings from current ``self``
        are propagated.

        :param data: Initial data.
        :param id: ID of instance. Ignored if *type* is not a
                   :class:`RedisCollection` subclass.
        :type id: string
        :param pipe: Redis pipe in case creation is performed as a part
                     of transaction. Ignored if *type* is not a
                     :class:`RedisCollection` subclass.
        :type pipe: :class:`redis.client.StrictPipeline` or
                    :class:`redis.client.StrictRedis`
        :param type: Type of the collection. Defaults to the same
                     type as ``self``.
        :type type: Class object.
        """
        cls = type or self.__class__
        if issubclass(cls, RedisCollection):
            return cls.fromanother(self, data, id, pipe)
        return cls(data)

    def _create_new_id(self):
        """Shorthand for creating new ID with id's key.
        Used in transactions.

        :rtype: tuple of strings (id, key)
        """
        id = self._create_id()
        return (
            id,
            self._create_key(id, prefix=self.prefix)
        )

    def _init_data(self, data, pipe=None):
        """Data initialization helper.

        :param data: Initial data.
        :param pipe: Redis pipe in case creation is performed as a part
                     of transaction.
        :type pipe: :class:`redis.client.StrictPipeline` or
                    :class:`redis.client.StrictRedis`
        """
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

    def _create_id(self):
        """Creates default collection ID.

        :rtype: string

        .. note::
            :func:`uuid.uuid4` is used. If you are not satisfied with its
            `collision
            probability <http://stackoverflow.com/a/786541/325365>`_,
            make your own implementation by subclassing and overriding this
            method.
        """
        return uuid.uuid4().hex

    def _create_key_name(self, namespace, item, prefix=None):
        """
        Simple key name factory. Puts all the parts together.

        :param namespace: Key namespace.
        :type namespace: string
        :param item: Item in given *namespace*.
        :type item: string
        :param prefix: Key prefix to use when working with Redis.
        :type prefix: string or :obj:`None`
        :rtype: string
        """
        components = [
            prefix,
            '_redis_collections',
            '_' + namespace.lower(),
            item.lower(),
        ]
        return '.'.join(filter(None, components))

    def _create_key(self, id, type=None, prefix=None):
        """Creates Redis key for collection.

        :param id: Collection ID.
        :type id: string
        :param type: Type of the collection. Defaults to the same
                     type as ``self``. Only subclasses of
                     :class:`RedisCollection` are expected.
        :type type: Class object.
        :param prefix: Key prefix to use when working with Redis.
        :type prefix: string or :obj:`None`
        :rtype: string
        """
        type_name = (type or self.__class__).__name__
        return self._create_key_name(type_name, id, prefix=prefix)

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
        :rtype: anything serializable or :obj:`None`
        """
        if string is None or string == '':
            return None
        if not isinstance(string, basestring):
            msg = 'Only strings can be unpickled (%r given).' % string
            raise TypeError(msg)
        return self.pickler.loads(string)

    @abstractmethod
    def _update(self, data, pipe=None):
        """Helper for update operations.

        :param data: Data for update.
        :param pipe: Redis pipe in case update is performed as a part
                     of transaction.
        :type pipe: :class:`redis.client.StrictPipeline` or
                    :class:`redis.client.StrictRedis`
        """
        pass

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

    def _transaction(self, fn):
        """Helper simplifying code within watched transaction.

        Takes *fn*, function treated as a transaction. Returns whatever
        *fn* returns. ``self.key`` is watched. *fn* takes *pipe* as the
        only argument.
        """
        results = []

        def trans(pipe):
            results.append(fn(pipe))

        self.redis.transaction(trans, self.key)
        return results[0]

    def _transaction_with_new(self, fn):
        """Helper simplifying code within transaction which
        creates a new instance of a Redis collection.

        Takes *fn*, function treated as a transaction. Returns whatever
        *fn* returns. ``self.key`` and the new key are watched.
        *fn* takes *pipe* as the first argument and the new ID as the second.
        """
        results = []
        new_id, new_key = self._create_new_id()

        def trans(pipe):
            results.append(fn(pipe, new_id))

        self.redis.transaction(trans, self.key, new_key)
        return results[0]

    def copy(self, id=None):
        """Return a copy of the collection.

        :param id: ID of the new collection. Defaults to auto-generated.
        :type id: string
        """
        def copy_trans(pipe, new_id):
            data = self._data(pipe=pipe)  # retrieve
            pipe.multi()
            return self._create_new(data, id=new_id, pipe=pipe)  # save

        return self._transaction_with_new(copy_trans)

    def __repr__(self):
        cls_name = self.__class__.__name__
        return '<redis_collections.%s %s>' % (cls_name, self.id)
