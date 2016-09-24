.. _api:

API Documentation
=================

Redis Collections are composed of only several classes. All items listed below
are exposed as public API, so you can (and you should) import them directly
from ``redis_collections`` package.

.. automodule:: redis_collections.base

.. autoclass:: RedisCollection
    :members:
    :special-members:
    :exclude-members: __metaclass__, __weakref__

.. automodule:: redis_collections.dicts

.. autoclass:: Dict
    :members:
    :special-members:

.. autoclass:: Counter
    :members:
    :special-members:

.. autoclass:: DefaultDict
    :members:
    :special-members:

.. automodule:: redis_collections.lists

.. autoclass:: List
    :members:
    :special-members:

.. autoclass:: Deque
    :members:
    :special-members:

.. automodule:: redis_collections.sets

.. autoclass:: Set
    :members:
    :special-members:

.. automodule:: redis_collections.sortedsets

.. autoclass:: SortedSetCounter
    :members:
    :special-members:

.. automodule:: redis_collections.syncable
    :members:
