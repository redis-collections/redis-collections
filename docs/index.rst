.. Redis Collections documentation master file, created by
   sphinx-quickstart on Wed Jan  9 17:09:50 2013.

Redis Collections
=================

Redis Collections are a set of basic Python collections backed by Redis.

`Redis <http://redis.io/>`_ is a great key-value storage. There is well-designed `client for Python <https://github.com/andymccurdy/redis-py>`_, but sometimes working with it seems to be too *low-level*. You just call methods with names of corresponding Redis commands. Such approach is great when dealing with cutting edge software tasks, but if you write just a simple app or command line script for your own, you might appreciate a bit different interface.

Aim of this small library is to provide such interface when dealing with collections. Redis has support for several types: strings, hashes, lists, sets, sorted sets. Why not to bring them to Python in a *pythonic* way? ::

    >>> from redis_collections import Dict
    >>> d = Dict()
    >>> d['answer'] = 42
    >>> d
    <redis_collections.Dict at fe267c1dde5d4f648e7bac836a0168fe {'answer': 42}>
    >>> d.items()
    [('answer', 42)]

In my Redis I can see a ``hash`` structure under key ``fe267c1dde5d4f648e7bac836a0168fe``. Using :class:`dict`-like notation, it's value is following::

    {'answer': 'I42\n.'}

The value is pickled, because Redis can store only strings. On Python side, you can do almost any stuff you are used to do with standard :class:`dict` instances:

    >>> d.update({'hasek': 39, 'jagr': 68})
    >>> d
    <redis_collections.Dict at fe267c1dde5d4f648e7bac836a0168fe {'answer': 42, 'jagr': 68, 'hasek': 39}>
    >>> del d['answer']
    >>> d
    <redis_collections.Dict at fe267c1dde5d4f648e7bac836a0168fe {'jagr': 68, 'hasek': 39}>

Every such operation atomically changes data in Redis.

Installation
------------

Current version is |release|.

The Cheese Shop::

    pip install redis-collections

In case you have an adventurous mind, give a try to the source::

    pip install git+https://github.com/honzajavorek/redis-collections.git#egg=redis-collections

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
    If you provide your own key string, collection will be successfully created. If there is no key corresponding in Redis, it will be created and initialized as an empty collection. This means you can set up your own way of assigning unique keys dependent on your other code. For example, by using IDs of records from your relational database you can have exactly one unique collection in Redis for every record from your SQL storage.

Redis connection
----------------

By default, collection uses a new Redis connection with its default values, **which is highly inefficient, but needs no configuration**. If you wish to use your own :class:`Redis` instance, pass it in ``redis`` keyword argument::

    >>> from redis import StrictRedis
    >>> r = StrictRedis()
    >>> d = Dict(redis=r)
    >>> l = List(redis=r)  # using the same connection as Dict above

There are several operations between collections resulting into creation of new instances of Redis Collections. These new instances
always use the same Redis connection as the original object::

    >>> from redis import StrictRedis
    >>> from redis_collections import List
    >>> r = StrictRedis()
    >>> l = List([1, 2], redis=r)
    >>> l
    <redis_collections.List at 196e407f8fc142728318a999ec821368 [1, 2]>
    >>> l + [4, 5, 6]  # result is using the same connection
    <redis_collections.List at 7790ef98639043c9abeacc80c2de0b93 [1, 2, 4, 5, 6]>

If you wish to add a prefix to keys used as collection identification in Redis, use ``prefix`` keyword argument::

    >>> from redis_collections import List
    >>> l = List(prefix='madagascar.')
    >>> l.key
    'madagascar.db6081d57d9345ac8f853fc9ab648b2d'
    >>> d = Dict(key='antananarivo', prefix='madagascar.')
    >>> d.key
    'madagascar.antananarivo'

New instances of collections coming from operations between them use the same ``prefix``. It is propagated as well as Redis connection.

Pickling
--------

If you don't like the standard way of data serialization made by :mod:`pickle`, you can set your own. Use ``pickler`` keyword argument:

    >>> import pickle
    >>> l = List(pickler=pickle)  # this has no sense

*pickler* can be anything having two methods (or functions): :func:`dumps` for conversion of data to string, and :func:`loads` for the opposite direction. You can write your own module or class with such interface, or you can use one of those which are already available::

    >>> import json
    >>> l = List(pickler=json)

New instances of collections coming from operations between them use the same ``pickler``. It is propagated as well as key prefix or Redis connection.

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

.. automodule:: redis_collections.lists

.. autoclass:: List
    :members:
    :special-members:

.. automodule:: redis_collections.sets

.. autoclass:: Set
    :members:
    :special-members:

Author
------

My name is Jan (**Honza**) **Javorek**. See my `GitHub <https://github.com/honzajavorek/>`_ profile for further details. I have a `blog <http://honzajavorek.cz>`_ and `Twitter <https://twitter.com/honzajavorek>`_ account, but you wouldn't probably understand a word as it is in `Czech <https://en.wikipedia.org/wiki/Czech_language>`_ only.

License: ISC
------------

© 2013 Jan Javorek <jan.javorek@gmail.com>

This work is licensed under `ISC license <https://en.wikipedia.org/wiki/ISC_license>`_.

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

