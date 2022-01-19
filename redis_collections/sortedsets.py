"""
sortedsets
~~~~~~~~~~

The `sortedsets` module contains collections based on the
Redis `Sorted Set <https://redis.io/commands#sorted_set>`__ type.

Included collections are :class:`SortedSetCounter` and :class:`GeoDB`.

"""
from redis.client import Pipeline

from .base import RedisCollection


class SortedSetBase(RedisCollection):
    def _data(self, pipe=None):
        pipe = self.redis if pipe is None else pipe
        if isinstance(pipe, Pipeline):
            pipe.zrange(self.key, 0, -1, withscores=True)
            items = pipe.execute()[-1]
        else:
            items = pipe.zrange(self.key, 0, -1, withscores=True)

        return [(self._unpickle(member), score) for member, score in items]

    def _repr_data(self):
        items = ('{}: {}'.format(repr(k), repr(v)) for k, v in self.items())
        return '{{{}}}'.format(', '.join(items))

    # Magic methods

    def __contains__(self, member):
        """Return ``True`` if *member* is present, else ``False``."""
        score = self.redis.zscore(self.key, self._pickle(member))
        return score is not None

    def __iter__(self, pipe=None):
        """
        Return an iterator of ``(member, score)`` tuples from the collection.
        """
        pipe = self.redis if pipe is None else pipe

        return iter(self._data(pipe))

    def __len__(self, pipe=None):
        """Return the number of members in the collection."""
        pipe = self.redis if pipe is None else pipe

        return pipe.zcard(self.key)

    # Named methods

    def clear(self, pipe=None):
        self._clear(pipe=pipe)

    def copy(self, key=None):
        other = self.__class__(redis=self.redis, key=key)
        other.update(self)

        return other

    def discard_member(self, member, pipe=None):
        """
        Remove *member* from the collection, unconditionally.
        """
        pipe = self.redis if pipe is None else pipe
        pipe.zrem(self.key, self._pickle(member))

    def scan_items(self):
        """
        Yield each of the ``(member, score)`` tuples from the collection,
        without pulling them all into memory.

        .. warning::
            This method may return the same (member, score) tuple multiple
            times.
            See the `Redis SCAN documentation
            <http://redis.io/commands/scan#scan-guarantees>`_ for details.
        """
        for m, s in self.redis.zscan_iter(self.key):
            yield self._unpickle(m), s

    def update(self, other):
        """
        Update the collection with items from *other*. Accepts other
        :class:`SortedSetBase` instances, dictionaries mapping members to
        numeric scores, or sequences of ``(member, score)`` tuples.
        """

        def update_trans(pipe):
            pipe.multi()
            other_items = method(pipe=pipe) if use_redis else method()

            for member, score in other_items:
                pipe.zadd(self.key, {self._pickle(member): float(score)})

        watches = []
        if self._same_redis(other, RedisCollection):
            use_redis = True
            watches.append(other.key)
        else:
            use_redis = False

        if hasattr(other, 'items'):
            method = other.items
        elif hasattr(other, '__iter__'):
            method = other.__iter__

        self._transaction(update_trans, *watches)


class SortedSetCounter(SortedSetBase):
    """
    :class:`SortedSetCounter` is a collection based on the Redis
    `Sorted Set <http://redis.io/topics/data-types#sorted-sets>`_ type.
    Instances map a unique set of ``member`` objects to floating point
    ``score`` values.

        >>> ssc = SortedSetCounter([('earth', 300), ('mercury', 100)])
        >>> ssc.set_score('venus', 200)
        >>> ssc.get_score('venus')
        200.0

    When retrieving members they are returned in order by score:

        >>> ssc.items()
        [('mercury', 100.0), ('venus', 200.0), ('earth', 300.0)]

    Ranges of items by rank can be computed and returned efficiently, as can
    ranges by score:

        >>> ssc.items(min_rank=1)  # 'mercury' has rank 0
        [('venus', 200.0), ('earth', 300.0)]
        >>> ssc.items(min_score=99, max_score=299)
        [('mercury', 100.0), ('venus', 200.0)]

    Collections support the ``in`` operator, and can be iterated over:

        >>> 'mercury' in ssc
        True
        >>> list(ssc)
         [('mercury', 100.0), ('venus', 200.0), ('earth', 300.0)]
        >>> len(ssc)
        3

    .. note::
        The API for :class:`SortedSetCounter` does not attempt to match an
        existing Python collection's.

        - Unlike :class:`Dict` or :class:`Set` objects, equal numeric types are
          considered distinct when used as members. For example, a collection
          can contain both ``1`` and ``1.0``.

        - Unlike :class:`Counter` or :class:`collections.Counter` objects, only
          :class:`float` scores can be stored.
    """

    def __init__(self, *args, **kwargs):
        """
        Create a new SortedSetCounter object.

        If the first argument (*data*) is an iterable object, create the new
        SortedSetCounter with its elements as the initial data.

        :param data: Initial data.
        :type data: iterable or mapping
        :param redis: Redis client instance. If not provided, default Redis
                      connection is used.
        :type redis: :class:`redis.StrictRedis`
        :param key: Redis key for the collection. Collections with the same key
                    point to the same data. If not provided, a random
                    string is generated.
        :type key: str
        """
        data = args[0] if args else kwargs.pop('data', None)

        super().__init__(**kwargs)

        if data:
            self.update(data)

    def count_between(self, min_score=None, max_score=None):
        """
        Returns the number of members whose score is between *min_score* and
        *max_score* (inclusive).
        """
        min_score = float('-inf') if min_score is None else float(min_score)
        max_score = float('inf') if max_score is None else float(max_score)

        return self.redis.zcount(self.key, min_score, max_score)

    def discard_by_rank(self, min_rank=None, max_rank=None, pipe=None):
        pipe = self.redis if pipe is None else pipe

        min_rank = 0 if min_rank is None else min_rank
        max_rank = -1 if max_rank is None else max_rank

        pipe.zremrangebyrank(self.key, min_rank, max_rank)

    def discard_by_score(self, min_score=None, max_score=None, pipe=None):
        pipe = self.redis if pipe is None else pipe

        min_score = float('-inf') if min_score is None else float(min_score)
        max_score = float('inf') if max_score is None else float(max_score)

        pipe.zremrangebyscore(self.key, min_score, max_score)

    def discard_between(
        self,
        min_rank=None,
        max_rank=None,
        min_score=None,
        max_score=None,
    ):
        """
        Remove members whose ranking is between *min_rank* and *max_rank*
        OR whose score is between *min_score* and *max_score* (both ranges
        inclusive). If no bounds are specified, no members will be removed.
        """
        no_ranks = (min_rank is None) and (max_rank is None)
        no_scores = (min_score is None) and (max_score is None)

        # Default scope: nothing
        if no_ranks and no_scores:
            return

        # Scope widens to given score range
        if no_ranks and (not no_scores):
            return self.discard_by_score(min_score, max_score)

        # Scope widens to given rank range
        if (not no_ranks) and no_scores:
            return self.discard_by_rank(min_rank, max_rank)

        # Scope widens to score range and then rank range
        with self.redis.pipeline() as pipe:
            self.discard_by_score(min_score, max_score, pipe)
            self.discard_by_rank(min_rank, max_rank, pipe)
            pipe.execute()

    def get_score(self, member, default=None, pipe=None):
        """
        Return the score of *member*, or *default* if it is not in the
        collection.
        """
        pipe = self.redis if pipe is None else pipe
        score = pipe.zscore(self.key, self._pickle(member))

        if (score is None) and (default is not None):
            score = float(default)

        return score

    def get_or_set_score(self, member, default=0):
        """
        If *member* is in the collection, return its value. If not, store it
        with a score of *default* and return *default*. *default* defaults to
        0.
        """
        default = float(default)

        def get_or_set_score_trans(pipe):
            pipe.multi()
            pickled_member = self._pickle(member)
            pipe.zscore(self.key, pickled_member)
            score = pipe.execute()[-1]

            if score is None:
                pipe.zadd(self.key, {self._pickle(member): default})
                return default

            return score

        return self._transaction(get_or_set_score_trans)

    def get_rank(self, member, reverse=False, pipe=None):
        """
        Return the rank of *member* in the collection.
        By default, the member with the lowest score has rank 0.
        If *reverse* is ``True``, the member with the highest score has rank 0.
        """
        pipe = self.redis if pipe is None else pipe
        method = getattr(pipe, 'zrevrank' if reverse else 'zrank')
        rank = method(self.key, self._pickle(member))

        return rank

    def increment_score(self, member, amount=1):
        """
        Adjust the score of *member* by *amount*. If *member* is not in the
        collection it will be stored with a score of *amount*.
        """
        return self.redis.zincrby(
            self.key, float(amount), self._pickle(member)
        )

    def items_by_rank(
        self, min_rank=None, max_rank=None, reverse=False, pipe=None
    ):
        pipe = self.redis if pipe is None else pipe

        min_rank = 0 if min_rank is None else min_rank
        max_rank = -1 if max_rank is None else max_rank

        if reverse:
            results = pipe.zrevrange(
                self.key, min_rank, max_rank, withscores=True
            )
        else:
            results = pipe.zrange(
                self.key, min_rank, max_rank, withscores=True
            )

        return [(self._unpickle(member), score) for member, score in results]

    def items_by_score(
        self, min_score=None, max_score=None, reverse=False, pipe=None
    ):
        min_score = float('-inf') if min_score is None else float(min_score)
        max_score = float('inf') if max_score is None else float(max_score)

        if reverse:
            method = pipe.zrevrangebyscore
            args = self.key, max_score, min_score
        else:
            method = pipe.zrangebyscore
            args = self.key, min_score, max_score

        pipe = self.redis if pipe is None else pipe
        if isinstance(pipe, Pipeline):
            method(*args, withscores=True)
            results = pipe.execute()[-1]
        else:
            results = method(*args, withscores=True)

        return [(self._unpickle(member), score) for member, score in results]

    def items(
        self,
        min_rank=None,
        max_rank=None,
        min_score=None,
        max_score=None,
        reverse=False,
        pipe=None,
    ):
        """
        Return a list of ``(member, score)`` tuples whose ranking is between
        *min_rank* and *max_rank* AND whose score is between *min_score* and
        *max_score* (both ranges inclusive). If no bounds are specified, all
        items will be returned.
        """
        pipe = self.redis if pipe is None else pipe

        no_ranks = (min_rank is None) and (max_rank is None)
        no_scores = (min_score is None) and (max_score is None)

        # Default scope: everything
        if no_ranks and no_scores:
            ret = self.items_by_score(min_score, max_score, reverse, pipe)
        # Scope narrows to given score range
        elif no_ranks and (not no_scores):
            ret = self.items_by_score(min_score, max_score, reverse, pipe)
        # Scope narrows to given rank range
        elif (not no_ranks) and no_scores:
            ret = self.items_by_rank(min_rank, max_rank, reverse, pipe)
        # Scope narrows twice - once by rank and once by score
        else:
            results = self.items_by_rank(min_rank, max_rank, reverse, pipe)
            ret = []
            for member, score in results:
                if (min_score is not None) and (score < min_score):
                    continue
                if (max_score is not None) and (score > max_score):
                    continue
                ret.append((member, score))

        return ret

    def set_score(self, member, score, pipe=None):
        """
        Set the score of *member* to *score*.
        """
        pipe = self.redis if pipe is None else pipe
        pipe.zadd(self.key, {self._pickle(member): float(score)})


class GeoDB(SortedSetBase):
    """
    :class:`GeoDB` is a collection based on the Redis
    `Geo <https://redis.io/commands/#geo>`_ type.
    Instances map a unique set of ``place`` objects (specified by their
    latitude and longitude) to a
    `Geohash <https://en.wikipedia.org/wiki/Geohash>`_.

    This allows for quick approximations of distances between places, and
    for quick searching within a given radius.

    .. note::
        This class requires Redis server version 3.2.0 or greater.

    """

    def __init__(self, *args, **kwargs):
        data = args[0] if args else kwargs.pop('data', None)

        super().__init__(**kwargs)

        if data:
            self.update(data)

    def __iter__(self):
        # Larger than the circumference of the spherical earth, in km
        everything_radius = 50000

        for item in self.places_within_radius(
            latitude=0, longitude=0, radius=everything_radius
        ):
            yield {
                'place': item['place'],
                'latitude': item['latitude'],
                'longitude': item['longitude'],
            }

    def __getitem__(self, place):
        ret = self.get_location(place)
        if ret is None:
            raise KeyError(place)

        return ret

    def __setitem__(self, place, location):
        return self.set_location(
            place, location['latitude'], location['longitude']
        )

    def distance_between(self, place_1, place_2, unit='km'):
        """
        Return the great-circle distance between *place_1* and *place_2*,
        in the *unit* specified.

        The default unit is ``'km'``, but ``'m'``, ``'mi'``, and ``'ft'`` can
        also be specified.
        """
        pickled_place_1 = self._pickle(place_1)
        pickled_place_2 = self._pickle(place_2)
        return self.redis.geodist(
            self.key, pickled_place_1, pickled_place_2, unit=unit
        )

    def get_hash(self, place):
        """
        Return the Geohash of *place*.
        If it's not present in the collection, ``None`` will be returned
        instead.
        """
        pickled_place = self._pickle(place)
        return self.redis.geohash(self.key, pickled_place)[0]

    def get_location(self, place):
        """
        Return a dict with the coordinates *place*. The dict's keys are
        ``'latitude'`` and ``'longitude'``.
        If it's not present in the collection, ``None`` will be returned
        instead.
        """
        pickled_place = self._pickle(place)
        try:
            longitude, latitude = self.redis.geopos(self.key, pickled_place)[0]
        except (AttributeError, TypeError):
            return None

        return {'latitude': latitude, 'longitude': longitude}

    def places_within_radius(
        self, place=None, latitude=None, longitude=None, radius=0, **kwargs
    ):
        """
        Return descriptions of the places stored in the collection that are
        within the circle specified by the given location and radius.
        A list of dicts will be returned.

        The center of the circle can be specified by the identifier of another
        place in the collection with the *place* keyword argument.
        Or, it can be specified by using both the *latitude* and *longitude*
        keyword arguments.

        By default the *radius* is given in kilometers, but you may also set
        the *unit* keyword argument to ``'m'``, ``'mi'``, or ``'ft'``.

        Limit the number of results returned with the *count* keyword argument.

        Change the sorted order by setting the *sort* keyword argument to
        ``b'DESC'``.
        """
        kwargs['withdist'] = True
        kwargs['withcoord'] = True
        kwargs['withhash'] = False
        kwargs.setdefault('sort', 'ASC')
        unit = kwargs.setdefault('unit', 'km')

        # Make the query
        if place is not None:
            response = self.redis.georadiusbymember(
                self.key, self._pickle(place), radius, **kwargs
            )
        elif (latitude is not None) and (longitude is not None):
            response = self.redis.georadius(
                self.key, longitude, latitude, radius, **kwargs
            )
        else:
            raise ValueError(
                'Must specify place, or both latitude and longitude'
            )

        # Assemble the result
        ret = []
        for item in response:
            ret.append(
                {
                    'place': self._unpickle(item[0]),
                    'distance': item[1],
                    'unit': unit,
                    'latitude': item[2][1],
                    'longitude': item[2][0],
                }
            )

        return ret

    def set_location(self, place, latitude, longitude, pipe=None):
        """
        Set the location of *place* to the location specified by
        *latitude* and *longitude*.

        *place* can be any pickle-able Python object.
        """
        self._geoadd(longitude, latitude, self._pickle(place), pipe=pipe)

    def update(self, other):
        """
        Update the collection with items from *other*. Accepts other
        :class:`GeoDB` instances, dictionaries mapping places to
        ``{'latitude': latitude, 'longitude': longitude}`` dicts,
        or sequences of ``(place, latitude, longitude)`` tuples.
        """
        # other is another Sorted Set
        def update_sortedset_trans(pipe):
            pipe.multi()
            items = other._data(pipe=pipe) if use_redis else other._data()
            for member, score in items:
                pipe.zadd(self.key, {self._pickle(member): float(score)})

        # other is dict-like
        def update_mapping_trans(pipe):
            pipe.multi()
            items = other.items(pipe=pipe) if use_redis else other.items()
            for place, value in items:
                self.set_location(
                    place, value['latitude'], value['longitude'], pipe=pipe
                )

        # other is a list of tuples
        def update_tuples_trans(pipe):
            pipe.multi()
            items = (
                other.__iter__(pipe=pipe) if use_redis else other.__iter__()
            )
            for place, latitude, longitude in items:
                self.set_location(place, latitude, longitude, pipe=pipe)

        watches = []
        if self._same_redis(other, RedisCollection):
            use_redis = True
            watches.append(other.key)
        else:
            use_redis = False

        if isinstance(other, SortedSetBase):
            func = update_sortedset_trans
        elif hasattr(other, 'items'):
            func = update_mapping_trans
        elif hasattr(other, '__iter__'):
            func = update_tuples_trans

        self._transaction(func, *watches)
