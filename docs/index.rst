.. Redis Collections documentation master file, created by
   sphinx-quickstart on Wed Jan  9 17:09:50 2013.

Redis Collections
=================

`redis-collections` is a library that provides several Python collection types backed by Redis.
This exposes Redis functionality with a Pythonic interface, and provides a simple way to store Python objects across sessions and processes.

This library builds on `Redis <http://redis.io/>`_, the excellent key-value store, and on `redis-py <https://github.com/andymccurdy/redis-py>`_, the well-designed Python interface for it.

Installation and Usage
----------------------

To get started, install the library with `pip <https://pip.pypa.io/en/stable/>`_:

.. code:: shell

   pip install redis-collections


With the library installed, import one the collections and use it to store some data:

    >>> from redis_collections import Dict
    >>> d = Dict()
    >>> d['answer'] = 42
    >>> d
    <redis_collections.Dict at fe267c1dde5d4f648e7bac836a0168fe {'answer': 42}>
    >>> d.items()
    [('answer', 42)]

In Redis you will see can see a ``hash`` structure under key ``fe267c1dde5d4f648e7bac836a0168fe``.
That structure stores a field and value that corresponds to ``{'answer': 42}`` (the key and value are pickled, because Redis can store only strings).

In Python you'll find that the collections can do most everything their Python counterparts can:

    >>> d.update({'hasek': 39, 'jagr': 68})
    >>> d
    <redis_collections.Dict at fe267c1dde5d4f648e7bac836a0168fe {'answer': 42, 'jagr': 68, 'hasek': 39}>
    >>> del d['answer']
    >>> d
    <redis_collections.Dict at fe267c1dde5d4f648e7bac836a0168fe {'jagr': 68, 'hasek': 39}>

Persistence
-----------

By default, a Redis key is generated when you create a new collection.
If you specify a key when creating a collection you can retrieve what was stored there previously:

    >>> d.key
    fe267c1dde5d4f648e7bac836a0168fe
    >>> e = Dict(key='fe267c1dde5d4f648e7bac836a0168fe')
    >>> e
    <redis_collections.Dict at fe267c1dde5d4f648e7bac836a0168fe {'jagr': 68, 'hasek': 39}>

This should even work across processes, meaning if your Python script terminates, you can retrieve its data again from Redis.

Each collection allows you to delete its Redis key with the :func:`clear` method::

    >>> d.clear()
    >>> d.items()
    []


.. note::
    You may provide your own key string when creating a collection
    If there is no corresponding key already in Redis, one will be created.
    This means you can set up your own way of assigning unique keys dependent on your other code.
    For example, by using IDs of records from your relational database you can have exactly one unique collection in Redis for every record from your SQL storage.

Redis Connection
----------------

By default, collections use a new Redis connection with its default values.
This requires no configuration, but is inefficient if you plan to use multiple collections.
To share a connection with multiple collections, create one (with ``redis.StrictRedis``) and pass it using the ``redis`` keyword when creating the collections:

    >>> from redis import StrictRedis
    >>> conn = StrictRedis()
    >>> d = Dict(redis=conn)
    >>> l = List(redis=conn)  # using the same connection as Dict above

A collection's ``copy`` method creates new a instance that uses the same Redis connection as the original object::

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
Storing a mutable object like a ``list`` in a ``Dict`` or a ``Set`` can lead to surprising behavior.
Because of Python semantics, it's impossible to automatically write to Redis when such an object is retrieved and modified.

    >>> d = Dict({'key': [1, 2]})  # Store a mutable object
    >>> d['key'].append(3)  # Retrieve and modify the object
    >>> d['key']  # Retrieve the object from Redis again
    [1, 2]

To work with such objects you may use a ``Dict`` with ``writeback`` enabled.
This will keep a local cache that is flushed to Redis when the ``sync`` method is called.

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

Subclass customization
----------------------

Collections use :func:`uuid.uuid4` for generating unique keys.
If you are not satisfied with that function's `collision probability <http://stackoverflow.com/a/786541/325365>`_ you may sublclass a collection and override its :func:`_create_key` method.

If you don't like how  :mod:`pickle` does serialization, you may override the ``_pickle`` and ``_unpickle`` methods of the collection classes.
Using other serializers may limit the objects you can store or retrieve.

Security considerations
-----------------------

Collections use :mod:`pickle`, which means you should never retrieve data from a source you don't trust.

For example: suppose you maintain a web application that has user profiles.
Users can submit their name, birthday, and a brief biography; and ultimately this is information stored in a Redis hash.
*Do not* attach a ``redis_collection.Dict`` instance to that hash key - a user could construct a string that gives them the ability to execute arbitrary code with your Python process's privileges.

Philosophy
----------

*   All operations are atomic. Race conditions are bugs - please `report them <https://github.com/honzajavorek/redis-collections/issues>`_.

*   Redis Collections stick to API of the original data structures known from Python standard library.
    To have the same (expected) behaviour is considered to be more important than efficiency.

    If there is more efficient approach than the one complying with the model interface, a new method exposing this approach should be introduced.

    If a collection's behavior doesn't match its standard Python counterpart, please `create an issue <https://github.com/honzajavorek/redis-collections/issues>`_.

*   Cases where different than standard approach would lead to better efficiency are mentioned and highlighted in API documentation as notes.
    Known incompatibilities with the original API are marked as warnings.

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

Changelog
---------

**→** :ref:`changelog`

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
