# -*- coding: utf-8 -*-
"""
base
~~~~
"""


import redis
from abc import ABCMeta, abstractmethod
from uuid import uuid4

try:
    import cPickle as pickle
except ImportError:
    import pickle as pickle


class RedisCollection:
    """Abstract class providing backend functionality for all the other
    Redis collections.
    """

    __metaclass__ = ABCMeta

    max_key_creation_attempts = 3

    not_impl_msg = ('Cannot be implemented efficiently or atomically '
                    'due to limitations in Redis command set.')

    @abstractmethod
    def __init__(self, redis=None, id=None, pickler=None, prefix=None):
        """Initializes the collection.

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
        """
        #: Redis client instance. :class:`StrictRedis` object with default
        #: connection settings is used if not set by :func:`__init__`.
        self.redis = redis or self._redis_factory()

        #: Class or module implementing pickling. Standard :mod:`pickle`
        #: module is set as default.
        self.pickler = pickler or pickle

        #: Redis key prefix. Default is empty string.
        self.prefix = prefix

        if id:
            # summoning existing collection
            key, id = self._key_factory(id=str(id), prefix=prefix)
        else:
            key, id = self._key_factory(prefix=prefix)

        #: Key used for this collection in Redis.
        self.key = key

        #: ID of the collection.
        self.id = id

    def _id_factory(self):
        """Creates default collection ID using :mod:`uuid`.

        :rtype: string
        """
        return str(uuid4().hex)

    def _redis_factory(self):
        """Creates default Redis connection.

        :rtype: :class:`redis.StrictRedis`
        """
        return redis.StrictRedis()

    def _key_factory(self, id=None, prefix=None):
        """Creates Redis key for the collection. If ID is not provided, factory
        searches only for a key which is not ocuppied yet. If it cannot be
        found, :exc:`RuntimeError` is raised.

        :param id: Collection ID.
        :type id: string or :obj:`None`
        :param prefix: Key prefix to use when working with Redis.
        :type prefix: string or :obj:`None`
        :rtype: tuple of strings (key, id)
        """
        def create_key(id):
            name = self.__class__.__name__.lower()
            components = [prefix, '_redis_collections', '_' + name, id]
            return '.'.join(filter(None, components))

        if id:
            return (create_key(id), id)

        for attempt in range(0, self.max_key_creation_attempts):
            id = self._id_factory()
            key = create_key(id)

            # repeat until an empty key is not found
            if not self.redis.exists(key):
                return (key, id)

        raise RuntimeError('Unable to find a free Redis key.')

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

    def _create_instance(self, values=None, type=None, id=None):
        """Creates instance of a collection. If subclass of
        :class:`RedisCollection` is requested, the same settings
        are applied as were for the current object. Otherwise no extra
        settings are passed, only *values* are propagated.

        :param values: Initial values.
        :param type: Type of the collection. Defaults to the same
                     type as ``self``.
        :param id: ID of requested instance. Ignored if *type* is
                   not a :class:`RedisCollection` subclass.
        """
        collection_type = type or self.__class__
        settings = {}

        if issubclass(collection_type, RedisCollection):
            settings = {
                'redis': self.redis,
                'pickler': self.pickler,
                'prefix': self.prefix,
                'id': id,
            }
        return collection_type(values, **settings)

    def _init(self, data, pipe=None):
        """Helper for init operations.

        :param data: Data for initialization.
        :param pipe: Redis pipe in case update is performed as a part
                     of transaction.
        """
        if data is not None:
            if not pipe:
                exc_pipe = True
                pipe = self.redis.pipeline()

            self._clear(pipe=pipe)
            if data:
                # non-empty data
                self._update(data, pipe=pipe)

            if exc_pipe:
                pipe.execute()

    @abstractmethod
    def _update(self, data, pipe=None):
        """Helper for update operations.

        :param data: Data for update.
        :param pipe: Redis pipe in case update is performed as a part
                     of transaction.
        """
        pass

    def _clear(self, pipe=None):
        """Helper for clear operations.

        :param pipe: Redis pipe in case update is performed as a part
                     of transaction.
        """
        redis = pipe or self.redis
        redis.delete(self.key)

    def clear(self):
        """Completely cleares the collection's data."""
        self._clear()

    def __repr__(self):
        cls_name = self.__class__.__name__
        return '<redis_collections.%s %s>' % (cls_name, self.id)
