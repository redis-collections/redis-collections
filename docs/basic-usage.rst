.. _basic-usage:

Examples
========

Basic usage
-----------

All collections can be imported from the top-level ``redis_collections``
package. For example, to use a Redis-backed version of Python's ``dict``,
import ``Dict`` and use it just like you would its Python counterpart:

.. code-block:: python

    >>> from redis_collections import Dict

    >>> D = Dict()
    >>> D['answer'] = 42  # Store an item on the Redis server
    >>> D['answer']  # Retrieve an item from the Redis server
    42

To use a dictionary that pushes its least recently used items to Redis, import
``LRUDict`` and use it like a normal ``dict``:

.. code-block:: python

    >>> from redis_collections import LRUDict

    >>> lru_dict = LRUDict(maxsize=2)
    >>> lru_dict.update([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
    >>> lru_dict['b']  # Most recently used key is now 'b'
    1
    >>> lru_dict['c'] = -2  # Most recently used key is now 'c'
    >>> lru_duct['e'] = 5  # 'e' pushes 'b' to Redis


Standard collections
--------------------

This package provides several collections that emulate built-in Python types
and classes from the `collections module
<https://docs.python.org/3/library/collections.html>`_.

=============== ===============  ==========
Collection      Python type      Redis type
=============== ===============  ==========
``Dict``        ``dict``         `Hash`
``List``        ``list``         `List`
``Set``         ``set``          `Set`
--------------- ---------------  ----------
``Counter``     ``Counter``      `Hash`
``DefaultDict`` ``defaultdict``  `Hash`
``Deque``       ``deque``        `List`
=============== ===============  ==========

Use each one as you would the standard version:

.. code-block:: python

    >>> from redis_collections import List, Set

    >>> L = List(key='test-list')
    >>> L.append('one')
    >>> L.extend(['two', 'three'])
    >>> L[:-1]
    ['one', 'two']

    >>> S = Set(key='test-set')
    >>> S.add('a')
    >>> S.add('a')
    >>> S.add('b')
    >>> len(S)
    2

Data is stored and retrieved in Redis, so if the Python process with a
collection terminates, you can re-load it in another process.

Syncable collections
--------------------

The standard Redis collections write to and read from Redis for all operations.
This means that all changes made in a Python process are reflected in Redis,
and that a collection made by one Python process can be accessed by another
Python process. However, this also means that all read and write operations are
fairly slow, since they have to retrieve data from and write data to Redis.

The syncable collections in this package provide types whose
contents are kept in memory. When their ``sync`` method is called those contents
are written to Redis:

.. code-block:: python

    >>> from redis_collections import SyncableDict

    >>> D = SyncableDict()
    >>> D['a'] = 1  # No write to Redis
    >>> D['a'] += 1  # No read from or write to Redis
    >>> D.sync()  # Contents are written to Redis

These collections can also be used with a ``with`` block for automatic
synchronization:

.. code-block:: python

    >>> with SyncableDict() as D:
    ...     D['a'] = 1
    ...     D['a'] += 1
    >>> D['a']  # Contents were written to Redis at the end of the with block
    2

If the Python process with a collection terminates, un-synchronized data won't
be available in Redis.

Other collections
-----------------

Least recently used dictionary
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The standard collections do their processing in Redis (at the expense of
speed) and the syncable collections do their processing in Python (at the
expense of automatic persistence).

The ``LRUDict`` collection provides a compromise. Recently used items are
stored in memory; older items are pushed to Redis:

.. code-block:: python

    >>> from redis_collections import LRUDict

    >>> D = LRUDict(maxsize=2)
    >>> D['a'] = 1
    >>> D['b'] = 2
    >>> D['c'] = 2  # 'a' is pushed to Redis and 'c' is stored locally
    >>> D['a']  # 'b' is pushed to Redis and 'a' is retrieved for local storage 
    1
    >>> D.sync()  # All items are copied to Redis

See the API Docs for ``LRUDict`` for more details.

Sorted Set counter
^^^^^^^^^^^^^^^^^^

The standard and syncable collections allow for access to Redis data types
through corresponding Python data types. However, there are Redis data types
that don't have an analog in Python.

The ``SortedSetCounter`` provides access to the Redis
`Sorted Set <http://redis.io/topics/data-types#sorted-sets>`_ type. Its API
doesn't emulate any Python type's, but should be easy for Python users to
understand and use:

.. code-block:: python

    >>> from redis_collections import SortedSetCounter

    >>> ssc = SortedSetCounter([('earth', 300), ('mercury', 100)])
    >>> ssc.set_score('venus', 200)
    >>> ssc.get_score('venus')
    200.0
    >>> ssc.items()
    [('mercury', 100.0), ('venus', 200.0), ('earth', 300.0)]

See the API Docs for ``SortedSetCounter`` for more details.
