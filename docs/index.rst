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
    <redis_collections.Dict fe267c1dde5d4f648e7bac836a0168fe>
    >>> d.items()
    [('answer', 42)]

In my Redis I can see a ``hash`` under key ``_redis_collections._dict.fe267c1dde5d4f648e7bac836a0168fe``. Using :class:`dict`-like notation, it's value is following::

    {'answer': 'I42\n.'}

The value is pickled, because Redis can store only strings mostly. On Python side, you can do almost any stuff you are used to do with standard :class:`dict` instances:

    >>> d.update({'hasek': 39, 'jagr': 68})
    >>> dict(d.items())
    {'answer': 42, 'jagr': 68, 'hasek': 39}
    >>> del d['answer']
    >>> dict(d.items())
    {'jagr': 68, 'hasek': 39}

Every such operation atomically changes data in Redis.

Installation
------------

Once complete, Redis Collections will be available on PyPI (probably as ``redis-collections``).

Persistence
-----------

When creating the :class:`Dict` object, your collection gets an unique ID. As you maybe noticed in previous examples, it is used in Redis key. If you keep this ID, you can summon your collection any time in the future:

    >>> d.id
    fe267c1dde5d4f648e7bac836a0168fe
    >>> d = None
    >>> d = Dict(id='fe267c1dde5d4f648e7bac836a0168fe')
    >>> dict(d.items())
    {'jagr': 68, 'hasek': 39}

In case you wish to wipe all its data, use :func:`clear` method, which is available to all collections provided by this library::

    >>> d.clear()
    >>> d.items()
    []

If I look to my Redis, key ``_redis_collections._dict.fe267c1dde5d4f648e7bac836a0168fe`` completely disappeared.

.. note::
    If you provide your own ID string, collection will be successfully created. If there is no key corresponding to such ID in Redis, it will be created and initialized as an empty collection. This means you can set up your own way of assigning unique keys dependent on your other code. For example, by using IDs of records from your relational database you can have exactly one unique collection in Redis for every record from your SQL storage.

If you wish to add a prefix to keys used as collection identification in Redis, use ``prefix`` keyword argument::

    >>> from redis_collections import List
    >>> l = List(prefix='antananarivo')
    >>> l.key
    'antananarivo._redis_collections._list.db6081d57d9345ac8f853fc9ab648b2d'

Custom Redis connection
-----------------------

By default, collection uses a new Redis connection with its default values. If you wish to use your own :class:`Redis` instance, pass it in ``redis`` keyword argument::

    >>> from redis import StrictRedis
    >>> r = StrictRedis()
    >>> d = Dict(redis=r)
    >>> l = List(redis=r)  # using the same connection as Dict above

Pickling
--------

If you don't like the standard way of data serialization made by :mod:`pickle`, you can set your own. Use ``pickler`` keyword argument:

    >>> import pickle
    >>> l = List(pickler=pickle)  # this has no sense

*pickler* can be anything having two methods (or functions): :func:`dumps` for conversion of data to string, and :func:`loads` for the opposite direction. You can write your own module or class with such interface, or you can use one of those which are already available::

    >>> import json
    >>> l = List(pickler=json)

Additional notes
----------------

*   All operations are atomic.
*   Redis Collections try to stick to API of the original data structures known from pure Python.
    To have the same (expected) behaviour is considered to be more important than efficiency.

    .. warning::
        If a collection has the requested method, but does not behave as the original built-in and does not raise NotImplementedError, it is a bug. Please, `report it <https://github.com/honzajavorek/redis-collections/issues>`_.

*   Sometimes API is extended with a couple of extra methods to expose some more efficient approaches.
*   Cases where different than standard approach would lead to better efficiency are mentioned and highlighted in API documentation as notes. Known incompatibilities with the original API are marked as warnings.

API Documentation
-----------------

Redis Collections have only one module with several classes representing provided collections.

.. automodule:: redis_collections

.. autoclass:: RedisCollection
    :members:
    :special-members:
    :exclude-members: __metaclass__, __weakref__

.. autoclass:: Dict
    :members:
    :special-members:

.. autoclass:: List
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

