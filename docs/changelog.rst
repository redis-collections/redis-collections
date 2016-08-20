Changelog
=========

Versioning
----------

`redis-collections` is currently at version |release|.

A 1.0 release is planned. Before that happens:

- Releases with breaking changes will be tagged as 0.4.x, 0.5.x, etc.
- Bug fix releases will be tagged as 0.3.x

After 1.0 is released:

- Releases with breaking changes will be tagged as 2.0.0, 3.0.0, etc.
- Releases with new features will be tagged as 1.1.0, 1.2.0, etc.
- Bug fix releases will be tagged as 1.0.1, 1.0.2, etc.

Breaking changes from 0.2.x
---------------------------

0.3.0 introduced some breaking changes:

- ``List`` slicing, ``Set`` methods like ``union``, and ``Counter`` operator methods now return Python objects instead of creating new Redis collections at randomly-generated keys.

  For example, previously ``List([0, 1, 2])[:1]`` would create a new ``List`` and store its items in Redis - now it returns a Python ``list``.

  Methods like ``copy`` (all collections) and ``fromkeys`` (``Dict``) that allow you to specify a Redis key as a keyword argument will still create new Redis collections.

- The non-standard ``List.get`` method was removed, as the standard ``List.__getitem__`` function is no longer particularly inefficient.

- ``Dict`` and ``Set`` now treat numeric types (``int``, ``float``, ``complex``, ``Fraction``, ``Decimal``) differently.
  Previously it was possible to store, e.g., both ``1.0`` and ``1`` as ``Set`` elements or ``Dict`` keys, which is not possible with the Python equivalents.

  Similarly, when using Python 2, ``Dict`` and ``Set`` now treat ``unicode`` and ``str`` types differently.
  It's no longer possible to store, e.g. both ``u'a'`` and ``b'a'`` in the same collection, and the behavior now matches the Python 2 equivalents.

  See `PR #60 <https://github.com/honzajavorek/redis-collections/pull/61#issue-171307493>`_ for details.

New features in 0.3.x
---------------------

- `Slicing and indexing for List <https://github.com/honzajavorek/redis-collections/issues/55>`_ should now be complete - no methods raise ``NotImplementedError``.

- `Cross-process Dict access <https://github.com/honzajavorek/redis-collections/issues/58>`_ should now work for Python 3.3 and later again.

See the `Github milestone <https://github.com/honzajavorek/redis-collections/milestone/1>`_ for more details.
