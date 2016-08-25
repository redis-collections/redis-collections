
Redis Collections
=================

.. image:: https://travis-ci.org/honzajavorek/redis-collections.svg?branch=master
   :target: https://travis-ci.org/honzajavorek/redis-collections

.. image:: https://coveralls.io/repos/github/honzajavorek/redis-collections/badge.svg?branch=master
   :target: https://coveralls.io/github/honzajavorek/redis-collections?branch=master


Set of basic Python collections backed by Redis.

Installation
------------

.. code:: shell

   pip install redis-collections

Example
-------

Redis Collections are a simple, pythonic way how to access Redis structures:

.. code:: python

  >>> from redis_collections import Dict
  >>> d = Dict()
  >>> d['answer'] = 42
  >>> d
  <redis_collections.Dict at fe267c1dde5d4f648e7bac836a0168fe {'answer': 42}>
  >>> d.items()
  [('answer', 42)]
  >>> d.update({'hasek': 39, 'jagr': 68})
  >>> d
  <redis_collections.Dict at fe267c1dde5d4f648e7bac836a0168fe {'answer': 42, 'jagr': 68, 'hasek': 39}>
  >>> del d['answer']
  >>> d
  <redis_collections.Dict at fe267c1dde5d4f648e7bac836a0168fe {'jagr': 68, 'hasek': 39}>

Available collections are ``Dict``, ``List``, ``Set``, ``Counter``, and ``DefaultDict``.

Documentation
-------------

**→** `redis-collections.readthedocs.io <https://redis-collections.readthedocs.io/>`_

Maintainers
-----------

- Bo Bayles (`@bbayles <http://github.com/bbayles>`_)
- Honza Javorek (`@honzajavorek <http://github.com/honzajavorek>`_)

License: ISC
------------

© 2013-? Honza Javorek <mail@honzajavorek>

This work is licensed under `ISC license <https://en.wikipedia.org/wiki/ISC_license>`_.
