Redis Collections
=================

`redis-collections` is a Python library that provides a high-level
interface to `Redis <http://redis.io/>`_, the excellent key-value store.

The library exposes several collection types that interact with Redis,
including:

*   Persistent versions of the built-in Python :class:`dict`,
    :class:`list`, and :class:`set` types that store their contents in Redis.
*   Persistent versions of the :class:`defaultdict`, :class:`Counter`, and
    :class:`deque` types from the `collections module
    <https://docs.python.org/3/library/collections.html>`_ that store their
    contents in Redis.
*   Subclasses of several Python types whose instances can automatically or
    manually back up their contents to Redis.
*   Pythonic wrappers for Redis-specific data types such as `Sorted Sets`.
    
Narrative Documentation
-----------------------

.. toctree::
   :maxdepth: 2

   basic-usage.rst
   usage-notes.rst
   development.rst

API Documentation
-----------------

.. toctree::
   :maxdepth: 2

   api.rst

Development
-----------

.. toctree::
   :maxdepth: 2

   changelog.rst
   development.rst    

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
