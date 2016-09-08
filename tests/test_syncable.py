#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, unicode_literals

import unittest

from redis_collections import (
    SyncableDict,
    SyncableCounter,
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

if __name__ == '__main__':
    unittest.main()
