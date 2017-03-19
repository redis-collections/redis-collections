#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, unicode_literals
import unittest
import redis_collections as rc


class RedisCollectionsFactoryTest(unittest.TestCase):

    def test_factory(self):
        coll = [
            'Counter',
            'DefaultDict',
            'Deque',
            'Dict',
            'List',
            'LRUDict',
            'Set',
            'SortedSetCounter',
            'SyncableDict',
            'SyncableCounter',
            'SyncableDefaultDict',
            'SyncableDeque',
            'SyncableList',
            'SyncableSet',
        ]

        factory = rc.RedisCollectionsFactory.from_url("redis://")
        for c in coll:
            native = getattr(rc, c)
            factor = getattr(factory, c)
            self.assertIsInstance(factor(key=c), native)


if __name__ == '__main__':
    unittest.main()

