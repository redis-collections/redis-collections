.. _changelog:

Changelog
=========

Versioning
----------

`redis-collections` is currently at version |release|.

A 1.0 release is planned. Before that happens:

- Releases with significant new features or breaking changes will be tagged as
  0.6.x, 0.7.x, etc.
- Bug fix releases will be tagged as 0.5.x

After 1.0 is released:

- Releases with breaking changes will be tagged as 2.0.0, 3.0.0, etc.
- Releases with new features will be tagged as 1.1.0, 1.2.0, etc.
- Bug fix releases will be tagged as 1.0.1, 1.0.2, etc.

Breaking changes
----------------

0.5.x
^^^^^

The 3.0.x release of `redis-py` is now required. Since it's not
backward-compatible with older versions, this library had to change as well.

In addition, Python 3.3 is no longer supported.

0.4.x
^^^^^

Nothing should have broken from 0.3.x to 0.4.x.

0.3.x
^^^^^

0.3.0 introduced some breaking changes:

- ``List`` slicing, ``Set`` methods like ``union``, and ``Counter`` operator
  methods now return Python objects instead of creating new Redis collections
  at randomly-generated keys.

  For example, previously ``List([0, 1, 2])[:1]`` would create a new ``List``
  and store its items in Redis - now it returns a Python ``list``.

  Methods like ``copy`` (all collections) and ``fromkeys`` (``Dict``) that
  allow you to specify a Redis key as a keyword argument will still create new
  Redis Collections.

- The non-standard ``List.get`` method was removed, as the standard
  ``List.__getitem__`` method is no longer particularly inefficient.

- ``Dict`` and ``Set`` now treat numeric types (``int``, ``float``,
  ``complex``, ``Fraction``, ``Decimal``) differently.
  Previously it was possible to store, e.g., both ``1.0`` and ``1`` as ``Set``
  elements or ``Dict`` keys, which is not possible with the Python equivalents.

  Similarly, when using Python 2, ``Dict`` and ``Set`` now treat ``unicode``
  and ``str`` types differently.
  It's no longer possible to store, e.g. both ``u'a'`` and ``b'a'`` in the same
  collection, and the behavior now matches the Python 2 equivalents.

  See `PR #60
  <https://github.com/honzajavorek/redis-collections/pull/61#issue-171307493>`_
  for details.

New features
------------

0.4.x
^^^^^

0.4.1 includes:

- Redis-specific ``scan_`` methods on ``Dict`` (and its subclassess), ``Set``,
  and ``SortedSetCounter``. See
  `PR #97 <https://github.com/honzajavorek/redis-collections/pull/97>`_ for
  details.

- Collections no longer query Redis at instantiation - thanks @ArminGruner.


0.4.0 introduced several new collections:

- ``LRUDict`` is a dict-like collection that holds a limited number of items
  locally, pushing least-recently used items to a Redis Hash.

- ``SyncableDict``, ``SyncableList``, ``SyncableSet``, and others are
  collections that hold items locally (which speeds up operations),
  but can sync them with Redis (which provides persistence).

- ``SortedSetCounter`` is a Pythonic interface to the Redis Sorted Set
  structure.
  It behaves a bit like a Counter, but its values are restricted to
  floating point numbers.

See the API Documentation for more details.

Also:

- The non-standard ``Set.random_sample`` method now works for Redis servers
  running Redis < 2.6.0.
  See `PR #80 <https://github.com/honzajavorek/redis-collections/pull/80>`_ for
  details.


0.3.x
^^^^^

- `Slicing and indexing for List
  <https://github.com/honzajavorek/redis-collections/issues/55>`_ should now be
  complete - no methods raise ``NotImplementedError``.

- `Cross-process Dict access
  <https://github.com/honzajavorek/redis-collections/issues/58>`_ should now
  work for Python 3.3 and later again.

- `Deque <https://github.com/honzajavorek/redis-collections/issues/6>`_ was
  added.

See the `0.3.0 milestone in GitHub
<https://github.com/honzajavorek/redis-collections/milestone/1>`_ for more
details.
