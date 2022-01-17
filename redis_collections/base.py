"""
base
~~~~
"""
import abc
from decimal import Decimal
from fractions import Fraction
import pickle
import uuid

import redis

NUMERIC_TYPES = (int,) + (float, Decimal, Fraction, complex)


class RedisCollection(metaclass=abc.ABCMeta):
    """Abstract class providing backend functionality for all the other
    Redis collections.
    """

    not_impl_msg = (
        'Cannot be implemented efficiently or atomically '
        'due to limitations in Redis command set.'
    )

    @abc.abstractmethod
    def __init__(
        self,
        redis=None,
        key=None,
        pickle_protocol=pickle.HIGHEST_PROTOCOL,
        hmset_command='hmset',
    ):
        """
        :param data: Initial data.
        :param redis: Redis client instance. If not provided, a new Redis
                      connection is created.
        :type redis: :class:`redis.StrictRedis`
        :param key: The key at which the collection will be stored in Redis.
                    Collections with the same key point to the same data.
                    If not provided a random key is generated.
        :type key: str
        :param pickle_protocol: The version number of the pickle protocol to
                                use. The default is the highest version
                                supported by the current Python process.
        :param hmset_command: The command to use for setting multiple values
                              in a hash. By default this is ``'hmset'``, which
                              will raise a ``DeprecationWarning`` on recent
                              versions of `redis-py`. Set to `'hset'` to
                              avoid this warning.
        :type key: str
        """
        self.redis = self._create_redis() if redis is None else redis
        self._redis_version = None  # Determined if needed and cached
        self._redis_py_version = None  # Determined if needed and cached
        self._hset_command = hmset_command
        self.key = key or self._create_key()

        self.pickle_protocol = pickle_protocol

    def _create_redis(self):
        """
        Creates a new Redis connection when none is specified during
        initialization.

        :rtype: :class:`redis.StrictRedis`
        """
        return redis.StrictRedis()

    def _create_key(self):
        """
        Creates a random Redis key for storing this collection's data.

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
        """Helper for getting the collection's data within a transaction.

        :param pipe: Redis pipe in case creation is performed as a part
                     of transaction.
        :type pipe: :class:`redis.client.StrictPipeline` or
                    :class:`redis.client.StrictRedis`
        """

    def _pickle(self, data):
        """Converts given data to a bytes string.

        :param data: Data to be serialized.
        :type data: anything serializable
        :rtype: bytes
        """
        return pickle.dumps(data, protocol=self.pickle_protocol)

    def _pickle_3(self, data):
        # Several numeric types are equal, have the same hash, but nonetheless
        # pickle to different byte strings. This method reduces them down to
        # integers to help match with Python's behavior.
        # len({1.0, 1, complex(1, 0)}) == 1
        if isinstance(data, complex):
            int_data = int(data.real)
            if data == int_data:
                data = int_data
        elif isinstance(data, NUMERIC_TYPES):
            int_data = int(data)
            if data == int_data:
                data = int_data

        return pickle.dumps(data, protocol=self.pickle_protocol)

    def _unpickle(self, pickled_data):
        """Convert *pickled_data* to a Python object and return it.

        :param pickled_data: Serialized data.
        :type pickled_data: bytes
        :rtype: anything serializable
        """
        return pickle.loads(pickled_data) if pickled_data else None

    def _clear(self, pipe=None):
        """Helper for clear operations.

        :param pipe: Redis pipe in case update is performed as a part
                     of transaction.
        :type pipe: :class:`redis.client.StrictPipeline` or
                    :class:`redis.client.StrictRedis`
        """
        redis = self.redis if pipe is None else pipe
        redis.delete(self.key)

    @property
    def redis_version(self):
        # Set the Redis version if it's not already set.
        if self._redis_version is None:
            self._redis_version = tuple(
                int(x) for x in self.redis.info()['redis_version'].split('.')
            )

        return self._redis_version

    @property
    def redis_py_version(self):
        # Set the redis-py version if it's not already set.
        if self._redis_py_version is None:
            self._redis_py_version = tuple(
                int(x) for x in redis.__version__.split('.')
            )

        return self._redis_py_version

    def _same_redis(self, other, cls=None):
        cls = cls or self.__class__
        if not isinstance(other, cls):
            return False

        self_kwargs = self.redis.connection_pool.connection_kwargs
        other_kwargs = other.redis.connection_pool.connection_kwargs

        return (
            self_kwargs.get('host') == other_kwargs.get('host')
            and self_kwargs.get('port') == other_kwargs.get('port')
            and self_kwargs.get('path') == other_kwargs.get('path')
            and self_kwargs.get('db', 0) == other_kwargs.get('db', 0)
        )

    def _geoadd(self, longitude, latitude, data, pipe=None):
        pipe = self.redis if pipe is None else pipe
        if self.redis_py_version < (4, 0, 0):
            return pipe.geoadd(self.key, longitude, latitude, data)
        else:
            return pipe.geoadd(self.key, (longitude, latitude, data))

    def _hmset(self, mapping, pipe=None):
        pipe = self.redis if pipe is None else pipe
        cmd = getattr(pipe, self._hset_command)
        return cmd(self.key, mapping=mapping)

    def _normalize_index(self, index, pipe=None):
        """Convert negative indexes into their positive equivalents."""
        pipe = self.redis if pipe is None else pipe
        len_self = self.__len__(pipe)
        positive_index = index if index >= 0 else len_self + index

        return len_self, positive_index

    def _normalize_slice(self, index, pipe=None):
        """Given a :obj:`slice` *index*, return a 4-tuple
        ``(start, stop, step, fowrward)``. The first three items can be used
        with the ``range`` function to retrieve the values associated with the
        slice; the last item indicates the direction.
        """
        if index.step == 0:
            raise ValueError
        pipe = self.redis if pipe is None else pipe

        len_self = self.__len__(pipe)

        step = index.step or 1
        forward = step > 0
        step = abs(step)

        if index.start is None:
            start = 0 if forward else len_self - 1
        elif index.start < 0:
            start = max(len_self + index.start, 0)
        else:
            start = min(index.start, len_self)

        if index.stop is None:
            stop = len_self if forward else -1
        elif index.stop < 0:
            stop = max(len_self + index.stop, 0)
        else:
            stop = min(index.stop, len_self)

        if not forward:
            start, stop = min(stop + 1, len_self), min(start + 1, len_self)

        return start, stop, step, forward, len_self

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

    def __enter__(self):
        self.writeback = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.sync()

    def sync(self):
        pass

    @abc.abstractmethod
    def _repr_data(self):
        """
        Abstract method for subclasses to implement.
        Return a string appropriate for displaying the contents of the
        collection. Called by __repr__.
        """

    def __repr__(self):
        cls_name = self.__class__.__name__
        data = self._repr_data()
        return '<redis_collections.{} at {} {}>'.format(
            cls_name, self.key, data
        )
