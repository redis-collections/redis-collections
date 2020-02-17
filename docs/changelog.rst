.. _changelog:

Changelog
=========

Releases
--------

- 0.8.0:
    - **Version compatibility**: This library now supports `redis-py` versions 3.4.0 and above.
- 0.7.1:
    - **Version compatibility**: This is a bugfix release to pin the `redis-py` dependency to below 3.4.0. A future version will add support for `redis-py` 3.4.0 and above.
- 0.7.0:
    - **Breaking change**: This library now supports Python versions 3.4 and later. The last version with Python 2.7 support is 0.6.0.
- 0.6.0:
    - **Breaking change**: This library now requires a Redis server at version 2.8.0 or above.
    - **Breaking change**: This library now depends `redis-py` version 3.1.0 or above.
    - **New collection**: Added the ``GeoDB`` collection, a high-level interface for Redis's GEO commands.
- 0.5.2:
    - **Documentation updates**: This was a documentation-only release with no code changes from 0.5.0
- 0.5.1:
    - **Requirements updates**: This release added the `six` library to the ``setup.py`` requirements; it had been missing before.
- 0.5.0:
    - **Breaking change**: This library now requires `redis-py` 3.0.0+.
    - **Breaking change**: Data is now pickled using the highest protocol version supported by Python.
      You can specify the ``pickle_protocol`` with a keyword argument - see :ref:`usage-notes`.
      To connect to a collection created with an older version of this package, set ``pickle_protocol=None``. See also `PR #101 <https://github.com/honzajavorek/redis-collections/pull/101>`_.
    - **Python support**: Python 3.3 is no longer supported.
- 0.4.2:
    - **Domain sockets support**: This is a bugfix release that enables connecting to a Redis server over a Unix domain socket.
- 0.4.1:
    - **SCAN methods**: Redis-specific ``scan_`` methods on ``Dict`` (and its subclassess), ``Set``,
      and ``SortedSetCounter``. See `PR #97 <https://github.com/honzajavorek/redis-collections/pull/97>`_ for
      details.
    - **Initialization changes**: Collections no longer query Redis at instantiation - thanks to ArminGruner.
- 0.4.0:
    - **Syncable collections**: ``SyncableDict``, ``SyncableList``, ``SyncableSet``, and others are
      collections that hold items locally (which speeds up operations),
      but can sync them with Redis (which provides persistence).
    - **Offload to Redis**: ``LRUDict`` is a dict-like collection that holds a limited number of items
      locally, pushing least-recently used items to a Redis Hash.
    - **Sorted Set collection**: ``SortedSetCounter`` is a Pythonic interface to the Redis Sorted Set
      structure. It behaves a bit like a Counter, but its values are restricted to
      floating point numbers.
    - **Version compatibility**: ``Set.random_sample`` now works for Redis servers under version 2.6.0

See the `GitHub Releases page <https://github.com/honzajavorek/redis-collections/releases>`_ for information on earlier releases.

Versioning
----------

`redis-collections` is currently at version |release|.

A 1.0 release is planned. Before that happens:

- Releases with significant new features or breaking changes will be tagged as
  0.9.x, 0.10.x, etc.
- Bug fix releases will be tagged as 0.8.x

After 1.0 is released:

- Releases with breaking changes will be tagged as 2.0.0, 3.0.0, etc.
- Releases with new features will be tagged as 1.1.0, 1.2.0, etc.
- Bug fix releases will be tagged as 1.0.1, 1.0.2, etc.
