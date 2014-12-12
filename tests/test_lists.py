#!/usr/bin/env python
# -*- coding: utf-8 -*-


import unittest

from .base import RedisTestCase
from redis_collections import List


class ListTest(RedisTestCase):

    def create_list(self, *args, **kwargs):
        kwargs['redis'] = self.redis
        return List(*args, **kwargs)

    def test_init(self):
        l = self.create_list([1, 2, 3])
        self.assertEqual(list(l), [1, 2, 3])
        l = self.create_list('abc')
        self.assertEqual(list(l), ['a', 'b', 'c'])
        l = self.create_list()
        self.assertEqual(list(l), [])

    def test_in(self):
        l = self.create_list([1, 2, 3])
        self.assertTrue(2 in l)
        self.assertFalse(42 in l)
        self.assertFalse(2 not in l)
        self.assertTrue(42 not in l)

    def test_concat(self):
        l1 = self.create_list([1, 2, 3])
        l2 = self.create_list([1, 4, 5])
        self.assertEqual(list(l1 + l2), [1, 2, 3, 1, 4, 5])
        self.assertEqual(list(l1 * 2), [1, 2, 3, 1, 2, 3])
        self.assertEqual(list(2 * l1), [1, 2, 3, 1, 2, 3])
        self.assertEqual(list(l1 * 0), [])
        self.assertEqual(list(l1 * -1), [])

    def test_set_get_overflow(self):
        l = self.create_list([1, 2, 3])

        def get_overflow(l):
            return l[42]

        def set_overflow(l):
            l[42] = 5

        self.assertRaises(IndexError, get_overflow, l)
        self.assertRaises(IndexError, set_overflow, l)
        self.assertEqual(l.get(42), None)
        self.assertEqual(l.get(1), 2)

    def test_index_count(self):
        l = self.create_list([1, 2, 3])
        self.assertEqual(l[0], 1)
        self.assertEqual(l[1], 2)
        self.assertEqual(l[2], 3)
        self.assertEqual(l[-1], 3)
        self.assertEqual(l[-2], 2)
        self.assertEqual(l[-3], 1)
        self.assertRaises(IndexError, lambda: l[42])
        self.assertRaises(IndexError, lambda: l[-42])

        l = self.create_list([1, 2, 3, 2, 3])
        self.assertEqual(l.index(2), 1)
        self.assertEqual(l.index(2, 2), 3)
        self.assertRaises(ValueError, l.index, 2, 2, 3)
        self.assertEqual(l.count(2), 2)

    def test_slice(self):
        l = self.create_list([1, 2, 3])
        self.assertEqual(list(l[0:1]), [1])
        self.assertEqual(list(l[0:2]), [1, 2])
        self.assertEqual(list(l[:]), [1, 2, 3])
        self.assertEqual(list(l[0:-1]), [1, 2])
        self.assertEqual(list(l[1:]), [2, 3])
        self.assertEqual(list(l[1::1]), [2, 3])
        self.assertEqual(list(l[1::2]), [2])

    def test_len_min_max(self):
        l = self.create_list([1, 2, 3])
        self.assertEqual(len(l), 3)
        self.assertEqual(min(l), 1)
        self.assertEqual(max(l), 3)

    def test_mutable(self):
        l = self.create_list([1, 2, 3])
        l[2] = 42
        self.assertEqual(l[2], 42)
        l[1:] = []
        self.assertEqual(list(l), [1])
        l.append(2013)
        self.assertEqual(list(l), [1, 2013])

    def test_del(self):
        l = self.create_list([1, 2013])
        del l[0]
        self.assertEqual(list(l), [2013])
        del l[1:]
        self.assertEqual(list(l), [2013])
        l.append(5)
        self.assertEqual(list(l), [2013, 5])
        l[1] = 8
        self.assertEqual(list(l), [2013, 8])
        del l[1:]
        self.assertEqual(list(l), [2013])

    def test_extend_insert(self):
        l = self.create_list([2013])
        l.extend([4, 5, 6, 7])
        self.assertEqual(list(l), [2013, 4, 5, 6, 7])
        l.insert(0, 3)
        self.assertEqual(list(l), [3, 2013, 4, 5, 6, 7])    # insert does not replace

    def test_pop_remove(self):
        l = self.create_list([3, 4, 5, 6, 7])
        self.assertEqual(l.pop(), 7)
        self.assertEqual(list(l), [3, 4, 5, 6])
        self.assertEqual(l.pop(0), 3)
        self.assertEqual(list(l), [4, 5, 6])
        l.extend([4, 5, 6])
        l.remove(4)
        self.assertEqual(list(l), [5, 6, 4, 5, 6])

    def test_slice_trim(self):
        l = self.create_list([5, 6, 4, 5, 6])
        l[2:] = []
        self.assertEqual(list(l), [5, 6])

    def test_reverse(self):
        l = self.create_list([1, 2, 3])
        l.reverse()
        self.assertEqual(list(l), [3, 2, 1])

    def test_lset_issue(self):
        for l in ( [1], self.create_list([1]) ):
            l.insert(0, 5)
            self.assertEqual(list(l), [5, 1])
            l.insert(0, 6)
            self.assertEqual(list(l), [6, 5, 1])
            l.append(7)
            self.assertEqual(list(l), [6, 5, 1, 7])


if __name__ == '__main__':
    unittest.main()
