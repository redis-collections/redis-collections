.. development:

Development
===========

See the `GitHub project page
<https://github.com/redis-collections/redis-collections/>`_ for source code.

Philosophy
----------

*   All operations should be atomic. Please `report
    <https://github.com/redis-collections/redis-collections/issues>`_ race
    conditions.

*   The standard collections should match the API of their Python counterparts.
    For these collections, having the same (expected) behavior is more
    important than processing efficiency.

    If there is more efficient approach than the one complying with the model
    interface, a new method exposing this approach should be introduced and
    documented.

    If a collection's behavior doesn't match its standard Python counterpart,
    and there is no documented warning, please `create an issue
    <https://github.com/redis-collections/redis-collections/issues>`_.

*   Behavior of "nested" collections is **undefined**. It is not recommended
    to create such structures. Use a collection of keys instead.
