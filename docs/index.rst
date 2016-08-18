.. Redis Collections documentation master file, created by
   sphinx-quickstart on Wed Jan  9 17:09:50 2013.

Redis Collections
=================

Redis Collections are a set of basic Python collections backed by Redis.

`Redis <http://redis.io/>`_ is a great key-value storage. There is well-designed `client for Python <https://github.com/andymccurdy/redis-py>`_, but sometimes working with it seems to be too *low-level*. You just call methods with names of corresponding Redis commands. Such approach is great when dealing with cutting edge software tasks, but if you write just a simple app or command line script for your own, you might appreciate a bit different interface.

The aim of this library is to provide such interface when dealing with collections. Redis has support for several types: strings, hashes, lists, sets, sorted sets. Why not to bring them to Python in a *pythonic* way? ::

    >>> from redis_collections import Dict
    >>> d = Dict()
    >>> d['answer'] = 42
    >>> d
    <redis_collections.Dict at fe267c1dde5d4f648e7bac836a0168fe {'answer': 42}>
    >>> d.items()
    [('answer', 42)]

In Redis you will see can see a ``hash`` structure under key ``fe267c1dde5d4f648e7bac836a0168fe``. That structure stores a field and value that corresponds to ``{'answer': 42}``.  The value is pickled, because Redis can store only strings.

On the Python side, you can do most anything you can do with standard :class:`dict` instances:

    >>> d.update({'hasek': 39, 'jagr': 68})
    >>> d
    <redis_collections.Dict at fe267c1dde5d4f648e7bac836a0168fe {'answer': 42, 'jagr': 68, 'hasek': 39}>
    >>> del d['answer']
    >>> d
    <redis_collections.Dict at fe267c1dde5d4f648e7bac836a0168fe {'jagr': 68, 'hasek': 39}>

"Write" operations atomically change data in Redis.

Installation
------------

Current version is |release|.

.. code:: shell

   pip install redis-collections

Persistence
-----------

When creating the :class:`Dict` object, your collection gets a unique Redis key. If you keep this key, you can summon your collection any time in the future:

    >>> d.key
    fe267c1dde5d4f648e7bac836a0168fe
    >>> Dict(key='fe267c1dde5d4f648e7bac836a0168fe')
    <redis_collections.Dict at fe267c1dde5d4f648e7bac836a0168fe {'jagr': 68, 'hasek': 39}>

In case you wish to wipe all its data, use :func:`clear` method, which is available to all collections provided by this library::

    >>> d.clear()
    >>> d.items()
    []

If I look to my Redis, key ``fe267c1dde5d4f648e7bac836a0168fe`` completely disappeared.

.. note::
    If you provide your own key string, a collection will be successfully created. If there is no key corresponding in Redis, it will be created and initialized as an empty collection. This means you can set up your own way of assigning unique keys dependent on your other code. For example, by using IDs of records from your relational database you can have exactly one unique collection in Redis for every record from your SQL storage.

Redis Connection
----------------

By default, collections use a new Redis connection with its default values, **which is highly inefficient, but needs no configuration**. If you wish to use your own :class:`Redis` instance, pass it in ``redis`` keyword argument::

    >>> from redis import StrictRedis
    >>> r = StrictRedis()
    >>> d = Dict(redis=r)
    >>> l = List(redis=r)  # using the same connection as Dict above

A collection's ``copy`` method creates new instance that uses the same Redis connection as the original object::

    >>> conn = StrictRedis()
    >>> list_01 = List([1, 2], redis=conn)
    >>> list_01
    <redis_collections.List at 196e407f8fc142728318a999ec821368 [1, 2]>
    >>> list_02 = list_01.copy()  # result is using the same connection
    <redis_collections.List at 7790ef98639043c9abeacc80c2de0b93 [1, 2]>

Operations on two collections backed by different Redis servers will be performed in Python::

    >>> list_1 = List((1, 2, 3), redis=StrictRedis(port=6379))
    >>> list_2 = List((4, 5, 6), redis=StrictRedis(port=6380))
    >>> list_1.extend(list_2)


Synchronization
---------------
Storing a mutable object like a ``list`` in a ``Dict`` can lead to surprising behavior.
Because of Python semantics, it's impossible to automatically write to Redis when such an object is retrieved and modified.

    >>> d = Dict({'key': [1, 2]})  # Store a mutable object
    >>> d['key'].append(3)  # Retrieve and modify the object
    >>> d['key']  # Retrieve the object from Redis again
    [1, 2]

To work with such objects you may use a ``Dict`` with ``writeback`` enabled. This will keep a local cache that is flushed to Redis when the ``sync`` method is called.

    >>> d = Dict({'key': [1, 2]}, writeback=True)
    >>> d['key'].append(3)
    >>> d['key']  # Modifications are retrieved from the cache
    [1, 2, 3]  
    >>> d.sync()  # Flush cache to Redis

You may also use a ``with`` block to automatically call the ``sync`` method.

    >>> with Dict({'key': [1, 2]}) as d:
    ...     d['key'].append(3)
    >>> d['key']  # Changes were automatically synced
    [1, 2, 3]

The ``writeback`` option is automatically enabled for ``DefaultDict`` objects.

Pickling
--------

If you don't like the standard way of data serialization made by :mod:`pickle`, you may override the ``_pickle`` and ``_unpickle`` methods of the collection classes.
Using other serializers may limit the objects you can store or retrieve.

Known issues
------------

*   Storing objects that have the same hash (such as the float ``1.0`` and the int ``1``) in a ``Set`` can lead to surprising behavior. They can both be retrieved, unlike with a native Python ``set``. See `issue 49 <https://github.com/honzajavorek/redis-collections/issues/49>`_.

*   Support for Python 3 is in progress. Please `report <https://github.com/honzajavorek/redis-collections/issues>`_ any issues you find.

Philosophy
----------

*   All operations are atomic.

    .. warning::
        If an operation has race conditions, it is a bug. Please, `report it <https://github.com/honzajavorek/redis-collections/issues>`_.

*   Redis Collections stick to API of the original data structures known from Python standard library.
    To have the same (expected) behaviour is considered to be more important than efficiency.

    .. warning::
        If a collection has the method you want to use, but it does not behave as the original built-in and it does not raise NotImplementedError, then it is a bug. Please, `report it <https://github.com/honzajavorek/redis-collections/issues>`_.

    If there is more efficient approach than the one complying with the model interface, new method exposing this approach should be introduced.

*   Cases where different than standard approach would lead to better efficiency are mentioned and highlighted in API documentation as notes. Known incompatibilities with the original API are marked as warnings.
*   Behavior of **nested Redis Collections** containing other Redis Collections is **undefined**.
    It is not recommended to create such structures. Use collection of keys instead.

API Documentation
-----------------

Redis Collections are composed of only several classes. All items listed below are exposed as public API, so you can (and you should) import them directly from ``redis_collections`` package.

.. automodule:: redis_collections.base

.. autoclass:: RedisCollection
    :members:
    :special-members:
    :exclude-members: __metaclass__, __weakref__

.. automodule:: redis_collections.dicts

.. autoclass:: Dict
    :members:
    :special-members:

.. autoclass:: Counter
    :members:
    :special-members:

.. autoclass:: DefaultDict
    :members:
    :special-members:

.. automodule:: redis_collections.lists

.. autoclass:: List
    :members:
    :special-members:

.. automodule:: redis_collections.sets

.. autoclass:: Set
    :members:
    :special-members:

Maintainers
-----------

- Bo Bayles (`@bbayles <http://github.com/bbayles>`_)
- Honza Javorek (`@honzajavorek <http://github.com/honzajavorek>`_)

License: ISC
------------

© 2013-? Honza Javorek <mail@honzajavorek>

This work is licensed under `ISC license <https://en.wikipedia.org/wiki/ISC_license>`_.

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
