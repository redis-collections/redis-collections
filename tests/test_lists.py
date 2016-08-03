#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, unicode_literals

import unittest

from redis_collections import List

from .base import RedisTestCase


class ListTest(RedisTestCase):

    def create_list(self, *args, **kwargs):
        kwargs['redis'] = self.redis
        return List(*args, **kwargs)

    def test_init(self):
        for init in (self.create_list, list):
            # List from list
            L = init([1, 2, 3])
            self.assertEqual(list(L), [1, 2, 3])

            # List from str
            L = init('abc')
            self.assertEqual(list(L), ['a', 'b', 'c'])

            # List from bytes
            L = init(b'abc')
            self.assertEqual(list(L), [ord('a'), ord('b'), ord('c')])

            # Empty list
            L = init()
            self.assertEqual(list(L), [])

    def test_in(self):
        for init in (self.create_list, list):
            L = init([1, 2, 3])
            self.assertIn(2, L)
            self.assertNotIn(4, L)

    def test_concat(self):
        for init in (self.create_list, list):
            L_1 = init([1, 2, 3])
            L_2 = init([1, 4, 5])
            self.assertEqual(list(L_1 + L_2), [1, 2, 3, 1, 4, 5])
            self.assertEqual(list(L_1 * 2), [1, 2, 3, 1, 2, 3])
            self.assertEqual(list(2 * L_1), [1, 2, 3, 1, 2, 3])
            self.assertEqual(list(L_1 * 2), [1, 2, 3, 1, 2, 3])
            self.assertEqual(list(L_1 * 0), [])
            self.assertEqual(list(L_1 * -1), [])

    def test_set_get_overflow(self):
        L = self.create_list([1, 2, 3])

        with self.assertRaises(IndexError):
            L[42]

        with self.assertRaises(IndexError):
            L[42] = 4

        self.assertEqual(L.get(42), None)
        self.assertEqual(L.get(1), 2)

    def test_index_count(self):
        for init in (self.create_list, list):
            L = init([1, 2, 3])
            self.assertEqual(L[0], 1)
            self.assertEqual(L[1], 2)
            self.assertEqual(L[2], 3)
            self.assertEqual(L[-1], 3)
            self.assertEqual(L[-2], 2)
            self.assertEqual(L[-3], 1)
            self.assertRaises(IndexError, lambda: L[42])
            self.assertRaises(IndexError, lambda: L[-42])

            L = init([1, 2, 3, 2, 3])
            self.assertEqual(L.index(2), 1)
            self.assertEqual(L.index(2, 2), 3)
            self.assertRaises(ValueError, L.index, 2, 2, 3)
            self.assertEqual(L.count(2), 2)

    def test_slice(self):
        for init in (self.create_list, list):
            L = init([1, 2, 3])
            self.assertEqual(list(L[:]), [1, 2, 3])

            self.assertEqual(list(L[0:]), [1, 2, 3])
            self.assertEqual(list(L[1:]), [2, 3])
            self.assertEqual(list(L[2:]), [3])
            self.assertEqual(list(L[3:]), [])

            # TODO - make this test pass
            # self.assertEqual(list(L[:0]), [])
            self.assertEqual(list(L[:1]), [1])
            self.assertEqual(list(L[:2]), [1, 2])
            self.assertEqual(list(L[:3]), [1, 2, 3])
            self.assertEqual(list(L[:4]), [1, 2, 3])

    def test_len_min_max(self):
        for init in (self.create_list, list):
            L = init([1, 2, 3])
            self.assertEqual(len(L), 3)
            self.assertEqual(min(L), 1)
            self.assertEqual(max(L), 3)

            self.assertEqual(len(init([])), 0)

    def test_mutable(self):
        for init in (self.create_list, list):
            L = init([1, 2, 3])
            L[2] = 42
            self.assertEqual(L[2], 42)

            L[1:] = []
            self.assertEqual(list(L), [1])

            L.append(2013)
            self.assertEqual(list(L), [1, 2013])

    def test_del(self):
        for init in (self.create_list, list):
            L = init([1, 2013])

            del L[0]
            self.assertEqual(list(L), [2013])

            del L[1:]
            self.assertEqual(list(L), [2013])

            L.append(5)
            self.assertEqual(list(L), [2013, 5])

            L[1] = 8
            self.assertEqual(list(L), [2013, 8])

            del L[1:]
            self.assertEqual(list(L), [2013])

    def test_extend_insert(self):
        for init in (self.create_list, list):
            L = init([2013])
            L.extend([4, 5, 6, 7])
            self.assertEqual(list(L), [2013, 4, 5, 6, 7])

            # insert does not replace
            L.insert(0, 3)
            self.assertEqual(list(L), [3, 2013, 4, 5, 6, 7])

    def test_pop_remove(self):
        for init in (self.create_list, list):
            L = init([3, 4, 5, 6, 7])
            self.assertEqual(L.pop(), 7)
            self.assertEqual(list(L), [3, 4, 5, 6])
            self.assertEqual(L.pop(0), 3)
            self.assertEqual(list(L), [4, 5, 6])
            L.extend([4, 5, 6])
            L.remove(4)
            self.assertEqual(list(L), [5, 6, 4, 5, 6])

    def test_slice_trim(self):
        for init in (self.create_list, list):
            L = init([5, 6, 4, 5, 6])
            L[2:] = []
            self.assertEqual(list(L), [5, 6])

    def test_reverse(self):
        for init in (self.create_list, list):
            L = init([1, 2, 3])
            L.reverse()
            self.assertEqual(list(L), [3, 2, 1])

    def test_lset_issue(self):
        for init in (self.create_list, list):
            L = init([1])
            L.insert(0, 5)
            self.assertEqual(list(L), [5, 1])
            L.insert(0, 6)
            self.assertEqual(list(L), [6, 5, 1])
            L.append(7)
            self.assertEqual(list(L), [6, 5, 1, 7])


if __name__ == '__main__':
    unittest.main()
