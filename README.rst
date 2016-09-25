
Redis Collections
=================

.. image:: https://travis-ci.org/honzajavorek/redis-collections.svg?branch=master
   :target: https://travis-ci.org/honzajavorek/redis-collections

.. image:: https://coveralls.io/repos/github/honzajavorek/redis-collections/badge.svg?branch=master
   :target: https://coveralls.io/github/honzajavorek/redis-collections?branch=master



`redis-collections` is a Python library that provides a high-level
interface to `Redis <http://redis.io/>`_, the excellent key-value store.

Quickstart
----------

Install the library with ``pip install redis-collections``.

The standard collections (e.g. ``Dict``, ``List``, ``Set``) behave like their
Python counterparts:

.. code-block:: python

    >>> from redis_collections import Dict, List, Set
    
    >>> D = Dict()
    >>> D['answer'] = 42
    >>> D['answer']
    42

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

Available collections
---------------------

The library provides the collections described below. Import them from ``redis_collections``:

+---------------------+------------+-------------------------+----------------------------------------------------------+
| Collection          | Redis type | Operations in           | Description                                              |
+=====================+============+=========================+==========================================================+
| Dict                | Hash       | Redis                   | Emulates Python's ``dict``                               |
+---------------------+------------+-------------------------+----------------------------------------------------------+
| List                | List       | Redis                   | Emulates Python's ``list``                               |
+---------------------+------------+-------------------------+----------------------------------------------------------+
| Set                 | Set        | Redis                   | Emulates Python's ``set``                                |
+---------------------+------------+-------------------------+----------------------------------------------------------+
| Counter             | Hash       | Redis                   | Emulates Python's ``collections.Counter``                |
+---------------------+------------+-------------------------+----------------------------------------------------------+
| DefaultDict         | Hash       | Redis                   | Emulates Python's ``collections.defaultdict``            |
+---------------------+------------+-------------------------+----------------------------------------------------------+
| Deque               | List       | Redis                   | Emulates Python's ``collections.deque``                  |
+---------------------+------------+-------------------------+----------------------------------------------------------+
| LRUDict             | Hash       | Python                  | LRU algorithm pushes items from Python to Redis          |
+---------------------+------------+-------------------------+----------------------------------------------------------+
| SyncableDict        | Hash       | Python                  | ``dict`` subclass that syncs to Redis                    |
+---------------------+------------+-------------------------+----------------------------------------------------------+
| SyncableList        | List       | Python                  | ``list`` subclass that syncs to Redis                    |
+---------------------+------------+-------------------------+----------------------------------------------------------+
| SyncableSet         | Set        | Python                  | ``set`` subclass that syncs to Redis                     |
+---------------------+------------+-------------------------+----------------------------------------------------------+
| SyncableCounter     | Hash       | Python                  | ``collections.Counter`` subclass that syncs to Redis     |
+---------------------+------------+-------------------------+----------------------------------------------------------+
| SyncableDefaultDict | Hash       | Python                  | ``collections.defaultdict`` subclass that syncs to Redis |
+---------------------+------------+-------------------------+----------------------------------------------------------+
| SortedSetCounter    | Sorted Set | Redis                   | Restricted interface for Redis's Sorted Set              |
+---------------------+------------+-------------------------+----------------------------------------------------------+

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

© 2013-? Honza Javorek <mail@honzajavorek>

This work is licensed under `ISC license <https://en.wikipedia.org/wiki/ISC_license>`_.
