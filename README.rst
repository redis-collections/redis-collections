
Redis Collections
=================

.. image:: https://travis-ci.org/honzajavorek/redis-collections.png
   :target: https://travis-ci.org/honzajavorek/redis-collections

Set of basic Python collections backed by Redis.

Installation
------------

The Cheese Shop::

    pip install redis-collections

In case you have an adventurous mind, give a try to the source::

    pip install git+https://github.com/honzajavorek/redis-collections.git#egg=redis-collections

Example
-------

Redis Collections are a simple, pythonic way how to access Redis structures::

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

Available collections are ``Dict``, ``List``, ``Set``, ``Counter``.

Documentation
-------------

**→** `redis-collections.readthedocs.org <https://redis-collections.readthedocs.org/>`_

License: ISC
------------

© 2013 Jan Javorek <jan.javorek@gmail.com>

This work is licensed under `ISC license <https://en.wikipedia.org/wiki/ISC_license>`_.
