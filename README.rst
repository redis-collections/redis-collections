
Redis Collections
=================

`redis-collections` is a Python library that provides a high-level
interface to `Redis <http://redis.io/>`_, the excellent key-value store.

Quickstart
----------

Install the library with ``pip install redis-collections``.
Import the collections from the top-level ``redis_collections`` package.

Standard collections
^^^^^^^^^^^^^^^^^^^^

The standard collections (e.g. ``Dict``, ``List``, ``Set``) behave like their
Python counterparts:

.. code-block:: python

    >>> from redis_collections import Dict, List, Set

    >>> D = Dict()
    >>> D['answer'] = 42
    >>> D['answer']
    42

+---------------------+------------+------------------------------------------------------+
|  Collection         | Redis type | Description                                          |
+=====================+============+======================================================+
| ``Dict``            | Hash       | Emulates Python's ``dict``                           |
+---------------------+------------+------------------------------------------------------+
| ``List``            | List       | Emulates Python's ``list``                           |
+---------------------+------------+------------------------------------------------------+
| ``Set``             | Set        | Emulates Python's ``set``                            |
+---------------------+------------+------------------------------------------------------+
| ``Counter``         | Hash       | Emulates Python's ``collections.Counter``            |
+---------------------+------------+------------------------------------------------------+
| ``DefaultDict``     | Hash       | Emulates Python's ``collections.defaultdict``        |
+---------------------+------------+------------------------------------------------------+
| ``Deque``           | List       | Emulates Python's ``collections.deque``              |
+---------------------+------------+------------------------------------------------------+

Syncable collections
^^^^^^^^^^^^^^^^^^^^

The syncable collections in this package provide types whose
contents are kept in memory. When their ``sync`` method is called those
contents are written to Redis:

.. code-block:: python

    >>> from redis_collections import SyncableDict

    >>> with SyncableDict() as D:
    ...     D['a'] = 1  # No write to Redis
    ...     D['a'] += 1  # No read from or write to Redis
    >>> D['a']  # D.sync() is called at the end of the with block
    2

+-------------------------+-----------------------------+-----------------------+
| Collection              | Python type                 | Description           |
+=========================+=============================+=======================+
| ``SyncableDict``        | ``dict``                    | Syncs to a Redis Hash |
+-------------------------+-----------------------------+-----------------------+
| ``SyncableList``        | ``list``                    | Syncs to a Redis List |
+-------------------------+-----------------------------+-----------------------+
| ``SyncableSet``         | ``set``                     | Syncs to a Redis Set  |
+-------------------------+-----------------------------+-----------------------+
| ``SyncableCounter``     | ``collections.Counter``     | Syncs to a Redis Hash |
+-------------------------+-----------------------------+-----------------------+
| ``SyncableDeque``       | ``collections.deque``       | Syncs to a Redis List |
+-------------------------+-----------------------------+-----------------------+
| ``SyncableDefaultDict`` | ``collections.defaultdict`` | Syncs to a Redis Hash |
+-------------------------+-----------------------------+-----------------------+

Other collections
^^^^^^^^^^^^^^^^^

The ``LRUDict`` collection stores recently used items in in memory.
It pushes older items to Redis:

.. code-block:: python

    >>> from redis_collections import LRUDict

    >>> D = LRUDict(maxsize=2)
    >>> D['a'] = 1
    >>> D['b'] = 2
    >>> D['c'] = 2  # 'a' is pushed to Redis and 'c' is stored locally
    >>> D['a']  # 'b' is pushed to Redis and 'a' is retrieved for local storage
    1
    >>> D.sync()  # All items are copied to Redis

The ``SortedSetCounter`` provides access to the Redis
`Sorted Set <http://redis.io/topics/data-types#sorted-sets>`_ type:

.. code-block:: python

    >>> from redis_collections import SortedSetCounter

    >>> ssc = SortedSetCounter([('earth', 300), ('mercury', 100)])
    >>> ssc.set_score('venus', 200)
    >>> ssc.get_score('venus')
    200.0
    >>> ssc.items()
    [('mercury', 100.0), ('venus', 200.0), ('earth', 300.0)]

Documentation
-------------

For more information, see
`redis-collections.readthedocs.io <https://redis-collections.readthedocs.io/>`_

Maintainers
-----------

- Bo Bayles (`@bbayles <http://github.com/bbayles>`_)
- Honza Javorek (`@honzajavorek <http://github.com/honzajavorek>`_)

License: ISC
------------

© 2016-? Bo Bayles <bbayles@gmail.com> and contributors
© 2013-2016 Honza Javorek <mail@honzajavorek.cz> and contributors

This work is licensed under `ISC license <https://en.wikipedia.org/wiki/ISC_license>`_.

This library is not affiliated with `Redis Labs <https://redislabs.com/>`__, `Redis <https://redis.io/>`__, or `redis-py <https://github.com/andymccurdy/redis-py>`__. Govern yourself accordingly!
