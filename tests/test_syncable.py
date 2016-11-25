#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, unicode_literals

import collections
import unittest

from redis_collections import (
    LRUDict,
    SyncableDict,
    SyncableCounter,
    SyncableDeque,
    SyncableDefaultDict,
    SyncableList,
    SyncableSet,
)

from .base import RedisTestCase


class SyncableTest(RedisTestCase):

    def create_collection(self, cls, *args, **kwargs):
        kwargs['redis'] = self.redis
        return cls(*args, **kwargs)

    def test_dict(self):
        dict_1 = self.create_collection(SyncableDict)

        # Setting a key should set it in Python but not in Redis
        dict_1['a'] = 1
        self.assertEqual(dict_1['a'], 1)
        self.assertNotIn('a', dict_1.persistence)

        # Synchronizing should set the key in Redis
        dict_1.sync()
        self.assertEqual(dict_1.persistence['a'], 1)

        # Deleting a key should remove it in Redis after sync
        dict_1['A'] = 'one'
        dict_1.sync()

        del dict_1['A']
        dict_1.sync()
        self.assertNotIn('A', dict_1.persistence)

        # Using a with block should automatically synchronize
        key_1 = dict_1.key
        with self.create_collection(SyncableDict, key=key_1) as dict_2:
            self.assertEqual(dict_2['a'], 1)

            dict_2['b'] = 2
        self.assertEqual(dict_2.persistence['b'], 2)

        # Modifying the second dict shouldn't affect the first
        self.assertNotEqual(dict_1, dict_2)

    def test_Counter(self):
        counter_1 = self.create_collection(SyncableCounter)

        counter_1['a'] = 1
        self.assertEqual(counter_1['a'], 1)
        self.assertNotIn('a', counter_1.persistence)

        counter_1.sync()
        self.assertEqual(counter_1.persistence['a'], 1)

        # Deleting a key should remove it in Redis after sync
        counter_1['A'] = 100
        counter_1.sync()

        del counter_1['A']
        counter_1.sync()
        self.assertNotIn('A', counter_1.persistence)

        key_1 = counter_1.key
        with self.create_collection(SyncableCounter, key=key_1) as counter_2:
            self.assertEqual(counter_2['a'], 1)

            counter_2['b'] = 2

        # Update is special for Counter - make sure contents can be retrieved
        # again unchanged.
        with self.create_collection(SyncableCounter, key=key_1) as counter_3:
            self.assertEqual(counter_3['a'], 1)
            self.assertEqual(counter_3['b'], 2)

        # The counter should behave like a Counter
        self.assertEqual(counter_3.most_common(), [('b', 2), ('a', 1)])

    def test_defaultdict(self):
        ddict_1 = self.create_collection(SyncableDefaultDict, int)

        # The first argument to defaultdict is the default_factory - it should
        # be set both for the defaultdict and the DefaultDict
        self.assertEqual(ddict_1.default_factory, int)
        self.assertEqual(ddict_1.persistence.default_factory, int)

        # The defaultdict should behave like a defaultdict
        self.assertEqual(ddict_1['a'], 0)
        ddict_1['a'] += 1
        self.assertEqual(ddict_1['a'], 1)
        self.assertNotIn('a', ddict_1.persistence)

        ddict_1.sync()
        self.assertEqual(ddict_1.persistence['a'], 1)

        # Deleting a key should remove it in Redis after sync
        ddict_1['A'] = 100
        ddict_1.sync()

        del ddict_1['A']
        ddict_1.sync()
        self.assertNotIn('A', ddict_1.persistence)

        # The default_factory can be changed between synchronizations
        key_1 = ddict_1.key
        with self.create_collection(
            SyncableDefaultDict, set, key=key_1
        ) as ddict_2:
            self.assertEqual(ddict_2['a'], 1)
            self.assertEqual(ddict_2.default_factory, set)

    def test_list(self):
        list_1 = self.create_collection(SyncableList)

        list_1.append('a')
        self.assertEqual(list_1, ['a'])
        self.assertEqual(list_1.persistence, [])

        list_1.sync()
        self.assertEqual(list_1.persistence, ['a'])

        key_1 = list_1.key
        with self.create_collection(SyncableList, key=key_1) as list_2:
            self.assertEqual(list_2, ['a'])

            list_2.append('b')

        self.assertEqual(list_2.persistence, ['a', 'b'])

        self.assertNotEqual(list_1, list_2)

    def test_deque(self):
        deque_1 = self.create_collection(SyncableDeque, [], 2)

        # Overflow the deque
        deque_1.append('a')
        deque_1.append('b')
        deque_1.append('c')
        self.assertEqual(deque_1, collections.deque(['b', 'c']))
        self.assertEqual(deque_1.persistence, collections.deque([]))

        # Sync and confirm contents are in Redis
        deque_1.sync()
        self.assertEqual(deque_1.persistence, collections.deque(['b', 'c']))

        # Associate a new deque with an existing one in Redis.
        # Modify the new one, make sure the old one isn't modified.
        kwargs = {'key': deque_1.key, 'maxlen': 2}
        with self.create_collection(SyncableDeque, **kwargs) as deque_2:
            self.assertEqual(deque_2, collections.deque(['b', 'c']))

            deque_2.append('a')

        self.assertEqual(deque_2.persistence, collections.deque(['c', 'a']))
        self.assertNotEqual(deque_1, deque_2)

    def test_set(self):
        set_1 = self.create_collection(SyncableSet)

        set_1.add('a')
        self.assertEqual(set_1, {'a'})
        self.assertEqual(set_1.persistence, set())

        set_1.sync()
        self.assertEqual(set_1.persistence, {'a'})

        key_1 = set_1.key
        with self.create_collection(SyncableSet, key=key_1) as set_2:
            self.assertEqual(set_2, {'a'})

            set_2.add('b')

        self.assertEqual(set_2.persistence, {'a', 'b'})

        self.assertNotEqual(set_1, set_2)


class LRUDictTest(RedisTestCase):

    def create_lru_dict(self, *args, **kwargs):
        kwargs['redis'] = self.redis
        return LRUDict(*args, **kwargs)

    def test_init(self):
        # maxsize defaults to None
        lru_dict_1 = self.create_lru_dict()
        self.assertIsNone(lru_dict_1.maxsize)
        lru_dict_1['a'] = 1
        lru_dict_1.sync()

        # Specifying another collection's key means its items should be
        # available
        lru_dict_2 = self.create_lru_dict(2, key=lru_dict_1.key)
        self.assertEqual(lru_dict_2.maxsize, 2)
        self.assertEqual(lru_dict_2['a'], 1)

    def test_contans(self):
        lru_dict = self.create_lru_dict(2)
        lru_dict.update([('a', 1), ('b', 2), ('c', 3)])

        # 'a' will be in Redis
        self.assertIn('a', lru_dict)

        # 'b' will be in local memory
        self.assertIn('b', lru_dict)
        self.assertIn('c', lru_dict)

        # 'd' will be in neither
        self.assertNotIn('d', lru_dict)

    def test_delitem(self):
        lru_dict = self.create_lru_dict(2)
        lru_dict.update([('a', 1), ('b', 2), ('c', 3)])

        # Delete from local memory
        self.assertIn('b', lru_dict.cache)
        del lru_dict['b']
        self.assertNotIn('b', lru_dict)
        self.assertNotIn('b', lru_dict.cache)
        self.assertNotIn('b', lru_dict.persistence)

        # Delete from Redis
        self.assertIn('a', lru_dict.persistence)
        del lru_dict['a']
        self.assertNotIn('a', lru_dict)
        self.assertNotIn('a', lru_dict.cache)
        self.assertNotIn('a', lru_dict.persistence)

    def test_getitem(self):
        lru_dict = self.create_lru_dict(3)
        data = [('a', 1), ('b', 2), ('c', 3), ('d', 4)]
        lru_dict.update(data)

        # maxsize is 3, and 4 items were stored
        self.assertIn('a', lru_dict.persistence)
        self.assertEqual(
            lru_dict.cache,
            collections.OrderedDict([('b', 2), ('c', 3), ('d', 4)])
        )

        # Retrieving a local item moves it to the rightmost position
        self.assertEqual(lru_dict['b'], 2)
        self.assertIn('a', lru_dict.persistence)
        self.assertEqual(
            lru_dict.cache,
            collections.OrderedDict([('c', 3), ('d', 4), ('b', 2)])
        )

        # Retrieiving a remote item brings it to the rightmost position in the
        # local cache
        self.assertEqual(lru_dict['a'], 1)
        self.assertIn('c', lru_dict.persistence)
        self.assertEqual(
            lru_dict.cache,
            collections.OrderedDict([('d', 4), ('b', 2), ('a', 1)])
        )

        self.assertEqual(lru_dict['c'], 3)
        self.assertIn('d', lru_dict.persistence)
        self.assertEqual(
            lru_dict.cache,
            collections.OrderedDict([('b', 2), ('a', 1), ('c', 3)])
        )

        # Retrieving unknown items raises KeyError
        self.assertRaises(KeyError, lambda: lru_dict['e'])

    def test_iter_len(self):
        lru_dict = self.create_lru_dict(3)
        data = [('a', 1), ('b', 2), ('c', 3), ('d', 4)]
        lru_dict.update(data)

        self.assertEqual(sorted(lru_dict), sorted(k for k, v in data))
        self.assertEqual(len(lru_dict), len(data))

    def test_setitem(self):
        lru_dict = self.create_lru_dict(2)

        # Add items up to the maxsize
        lru_dict['a'] = 1
        lru_dict['b'] = 2
        self.assertEqual(len(lru_dict.persistence), 0)
        self.assertEqual(
            lru_dict.cache, collections.OrderedDict([('a', 1), ('b', 2)])
        )

        # Adding a new item pushes out the oldest item
        lru_dict['c'] = 3
        self.assertIn('a', lru_dict.persistence)
        self.assertEqual(
            lru_dict.cache, collections.OrderedDict([('b', 2), ('c', 3)])
        )

        # Updating a local item moves it to the rightmost position
        lru_dict['b'] = -2
        self.assertIn('a', lru_dict.persistence)
        self.assertEqual(
            lru_dict.cache, collections.OrderedDict([('c', 3), ('b', -2)])
        )

        # Updating a Redis item moves it to the rightmost position
        lru_dict['a'] = -1
        self.assertIn('c', lru_dict.persistence)
        self.assertEqual(
            lru_dict.cache, collections.OrderedDict([('b', -2), ('a', -1)])
        )

    def test_setitem_unlimited(self):
        lru_dict = self.create_lru_dict()

        for i, k in enumerate(('a', 'b', 'c', 'd'), 1):
            lru_dict[k] = i
        self.assertEqual(lru_dict.cache, {'a': 1, 'b': 2, 'c': 3, 'd': 4})
        self.assertEqual(lru_dict.persistence, {})

    def test_clear(self):
        lru_dict = self.create_lru_dict(2)
        lru_dict.update([('a', 1), ('b', 2), ('c', 3)])

        self.assertEqual(len(lru_dict), 3)
        self.assertEqual(len(lru_dict.cache), 2)
        self.assertEqual(len(lru_dict.persistence), 1)

        lru_dict.clear()
        self.assertEqual(len(lru_dict), 0)
        self.assertEqual(len(lru_dict.cache), 0)
        self.assertEqual(len(lru_dict.persistence), 0)

    def test_copy(self):
        lru_dict_1 = self.create_lru_dict(2)
        data = [('a', 1), ('b', 2), ('c', 3)]
        lru_dict_1.update(data)
        lru_dict_2 = lru_dict_1.copy()

        self.assertTrue(lru_dict_1.redis is lru_dict_2.redis)
        self.assertNotEqual(lru_dict_1.key, lru_dict_2.key)
        self.assertEqual(dict(data), dict(lru_dict_2))

    def test_fromkeys(self):
        lru_dict_1 = self.create_lru_dict()
        lru_dict_2 = lru_dict_1.fromkeys(
            ['a', 'b', 'c'], value=0, maxsize=2, redis=lru_dict_1.redis
        )
        self.assertEqual(lru_dict_2.maxsize, 2)
        self.assertTrue(lru_dict_1.redis is lru_dict_2.redis)
        self.assertEqual(lru_dict_2, {'a': 0, 'b': 0, 'c': 0})

    def test_sync(self):
        lru_dict = self.create_lru_dict()
        data = [('a', 1), ('b', 2), ('c', 3)]
        lru_dict.update(data)

        # sync with no arguments copies everything to Redis
        self.assertEqual(lru_dict.persistence, {})
        lru_dict.sync()
        self.assertEqual(lru_dict.cache, dict(data))
        self.assertEqual(lru_dict.persistence, dict(data))

        # sync should happen automatically when using a with block
        with lru_dict as D:
            D['a'] = -1
            self.assertEqual(lru_dict.persistence['a'], 1)
        self.assertEqual(lru_dict.persistence['a'], -1)

        # sync with clear_cache should clear the local cache
        lru_dict['b'] = -2
        lru_dict.sync(clear_cache=True)
        self.assertEqual(lru_dict.persistence['b'], -2)
        self.assertEqual(len(lru_dict.cache), 0)


if __name__ == '__main__':
    unittest.main()
