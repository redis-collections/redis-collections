.. _usage-notes:

Usage notes
=================

Persistence
-----------

By default, a Redis key is generated when you create a new collection:

.. code-block:: python

    >>> from redis_collections import Dict

    >>> D = Dict()
    >>> D['answer'] = 42
    >>> D.key
    fe267c1dde5d4f648e7bac836a0168fe

If you specify a key when creating a collection you can retrieve what was
stored there previously:

.. code-block:: python

    >>> E = Dict(key='fe267c1dde5d4f648e7bac836a0168fe')
    >>> E['answer']
    42

This should even work across processes, meaning if your Python script
terminates, you can retrieve its data again from Redis.

Each collection allows you to delete its Redis key with the `clear` method:

.. code-block:: python

    >>> D.clear()
    >>> list(D.items())

.. note::
    Stored objects are serialized with Python-standard pickling.
    By default, the `highest protocol version
    <https://docs.python.org/3/library/pickle.html#pickle.HIGHEST_PROTOCOL>`_
    is used.
    It's not recommended to retrieve objects created by one version of Python
    with another version.
    If you attempt to do that, be sure to set the ``pickle_protocol`` keyword
    argument to a version that both Python versions support when
    declaring a collection.


Redis connection
----------------

By default, collections create a new Redis connection when they are
instantiated. This requires no configuration, but is inefficient if you are
using multiple collections. To share a connection among multiple collections,
create one (with ``redis.StrictRedis``) and pass it using the ``redis``
keyword when creating the collections.

.. code-block:: python

    >>> from redis import StrictRedis
    >>> conn = StrictRedis()
    >>> D = Dict(redis=conn)
    >>> L = List(redis=conn)

A collection's ``copy`` method creates new a instance that uses the same Redis
connection as the original object:

.. code-block:: python

    >>> conn = StrictRedis()
    >>> list_01 = List([1, 2], redis=conn)
    >>> list_02 = list_01.copy()  # result is using the same connection

Operations on two collections backed by different Redis servers will be
performed in Python:

.. code-block:: python

    >>> list_1 = List((1, 2, 3), redis=StrictRedis(port=6379))
    >>> list_2 = List((4, 5, 6), redis=StrictRedis(port=6380))
    >>> list_1.extend(list_2)

.. _Synchronization:

Synchronization
---------------
Storing a mutable object like a ``list`` in a ``Dict`` or a ``List`` can lead
to surprising behavior. Because of Python semantics, it's impossible to
automatically write to Redis when such an object is retrieved and modified.

.. code-block:: python

    >>> D = Dict({'key': [1, 2]})  # Store a mutable object
    >>> D['key'].append(3)  # Retrieve and modify the object
    >>> D['key']  # Retrieve the object from Redis again
    [1, 2]

If you plan to work with mutable objects, be sure to specify ``writeback=True``
when instantiating your collection. This will keep a local cache that is
flushed to Redis when the ``sync`` method is called:

.. code-block:: python

    >>> D = Dict({'key': [1, 2]}, writeback=True)
    >>> D['key'].append(3)
    >>> D['key']  # Modifications are retrieved from the cache
    [1, 2, 3]
    >>> D.sync()  # Flush cache to Redis

You may also use a ``with`` block to automatically call the ``sync`` method.

.. code-block:: python

    >>> with Dict({'key': [1, 2]}) as D:
    ...     D['key'].append(3)
    >>> D['key']  # Changes were automatically synced
    [1, 2, 3]

The ``writeback`` option is automatically enabled for ``DefaultDict`` objects.

.. _Hashing:

Hashing dictionary keys and set elements
----------------------------------------

Python `takes care
<https://docs.python.org/3/library/stdtypes.html#hashing-of-numeric-types>`_
to make sure that equal numeric values, such as ``1.0`` and ``1``, have the
same hash value. If you add ``1.0`` to a ``set`` or a ``dict``, you will not be
able to add ``1``, as an equal value is already stored.

The Redis-backed ``Dict`` and ``Set`` classes in this library attempt to follow
this behavior, but there are some differences. For the built-in Python
collections, you get back the first value you stored:

.. code-block:: python

    >>> python_dict = {}
    >>> python_dict[1.0] = 'one'  # 1.0 stored first
    >>> python_dict[1] = 'One'  # 1 stored second
    >>> list(python_dict.keys())  # 1.0 is retrieved
    [1.0]

For the Redis-backed collections, you'll get back the integer:

.. code-block:: python

    >>> redis_dict = Dict()
    >>> redis_dict[1.0] = 'one'  # 1.0 stored first
    >>> redis_dict[1] = 'One'  # 1 stored second
    >>> list(redis_dict.keys())  # 1 is retrieved
    [1]

This behavior applies to ``complex``, ``float``, ``Decimal``, and ``Fraction``
values that have an integer equivalent. It doesn't apply to values that don't
have an integer equivalent (such as ``1.1`` or ``complex(1, 1)``).

Security considerations
-----------------------

Collections use :mod:`pickle`, which means you should never retrieve data from
a source you don't trust.

For example: suppose you maintain a web application that has user profiles.
Users can submit their name, birthday, and a brief biography; and ultimately
this is information stored in a Redis `hash`. *Do not* attach a
``redis_collection.Dict`` instance to that hash key - a user could construct
a string that gives them the ability to execute arbitrary code with your Python
process's privileges.

Subclass customization
----------------------

Collections use :func:`uuid.uuid4` for generating unique keys.
If you are not satisfied with that function's
`collision probability <http://stackoverflow.com/a/786541/325365>`_ you may
sublclass a collection and override its :func:`_create_key` method.

If you don't like how  :mod:`pickle` does serialization, you may override the
``_pickle*`` and ``_unpickle*`` methods on the collection classes.
Using other serializers will limit the objects you can store or retrieve.

.. note::
    On Python 2, the :mod:`pickle` module is used instead of the
    :mod:`cPickle` module. This is intentional - see
    `issue #83 <https://github.com/honzajavorek/redis-collections/issues/83>`_.
