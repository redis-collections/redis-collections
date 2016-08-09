#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, unicode_literals

import unittest

import six

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
            if six.PY2:
                self.assertEqual(list(L), [b'a', b'b', b'c'])
            else:
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

        self.assertIsNone(L.get(42))
        self.assertEqual(L.get(42, 'MISSING'), 'MISSING')
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
        redis_list = self.create_list([0, 1, 2, 3])
        python_list = [0, 1, 2, 3]

        for index in [
            slice(None, None),  # L[:]
            slice(0, None, None),  # L[0:]
            slice(1, None, None),  # L[1:]
            slice(3, None, None),  # L[3:]
            slice(4, None, None),  # L[4:]
            slice(None, 0, None),  # L[:0]
            slice(None, 1, None),  # L[:1]
            slice(None, 3, None),  # L[:3]
            slice(None, 4, None),  # L[:3]
            slice(0, 0, None),  # L[0:0]
            slice(1, 1, None),  # L[1:1]
            slice(3, 3, None),  # L[3:3]
            slice(3, 3, None),  # L[3:3]
            slice(-1, None, None),  # L[-1:]
            slice(-2, None, None),  # L[-2:]
            slice(-4, None, None),  # L[-4:]
            slice(None, -1, None),  # L[-1:]
            slice(None, -2, None),  # L[-2:]
            slice(None, -4, None),  # L[-4:]
            slice(0, -1, None),  # L[0:-1]
            slice(1, -1, None),  # L[1:-1]
            slice(1, -2, None),  # L[1:-2]
            slice(-3, -1, None),  # L[-3:-1]
            slice(None, None, 1),  # L[::1]
            slice(None, None, 2),  # L[::2]
            slice(None, None, 3),  # L[::3]
            slice(None, None, 4),  # L[::4]
            slice(None, None, -1),  # L[::-1]
            slice(None, None, -2),  # L[::-2]
            slice(1, -1, 2),  # L[1:-1:1]
            slice(1, -1, -2),  # L[1:-1:-2]
        ]:
            self.assertEqual(list(redis_list[index]), python_list[index])

    def test_len_min_max(self):
        for init in (self.create_list, list):
            L = init([1, 2, 3])
            self.assertEqual(len(L), 3)
            self.assertEqual(min(L), 1)
            self.assertEqual(max(L), 3)

            self.assertEqual(len(init([])), 0)

    def test_modify(self):
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

            del L[:]
            self.assertEqual(list(L), [])

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

    def test_reversed(self):
        for init in (self.create_list, list):
            L = init([0, 1, 2, 3])
            self.assertEqual(list(reversed(L)), [3, 2, 1, 0])

    def test_mutable(self):
        redis_cached = self.create_list(writeback=True)
        python_list = []

        redis_cached.append({'one': 1})
        python_list.append({'one': 1})

        redis_cached[0]['one'] = 2
        python_list[0]['one'] = 2

        self.assertEqual(redis_cached[0], python_list[0])
        self.assertEqual(
            list(redis_cached), list(python_list)
        )
        self.assertEqual(
            list(reversed(redis_cached)), list(reversed(python_list))
        )

        # Changes are not reflected in Redis until after sync
        self.assertNotEqual(list(redis_cached._data())[0], python_list[0])
        redis_cached.sync()
        self.assertEqual(list(redis_cached._data())[0], python_list[0])
        self.assertEqual(redis_cached.cache, {})

    def test_cache(self):
        redis_cached = self.create_list(writeback=True)

        # append
        redis_cached.append([])
        redis_cached[0].append('whartnell')

        redis_cached.append([])
        redis_cached[1].append('ptroughton')

        redis_cached.append([])
        redis_cached[2].append('jpertwee')

        self.assertEqual(
            redis_cached.cache,
            {0: ['whartnell'], 1: ['ptroughton'], 2: ['jpertwee']}
        )

        # __iter__
        self.assertEqual(
            list(redis_cached),
            [['whartnell'], ['ptroughton'], ['jpertwee']]
        )

        # __getitem__
        self.assertEqual(redis_cached[0], ['whartnell'])
        self.assertEqual(redis_cached[1], ['ptroughton'])
        self.assertEqual(redis_cached[2], ['jpertwee'])

        self.assertEqual(redis_cached[-3], ['whartnell'])
        self.assertEqual(redis_cached[-2], ['ptroughton'])
        self.assertEqual(redis_cached[-1], ['jpertwee'])

        # get
        self.assertEqual(redis_cached.get(0), ['whartnell'])

        # __setitem__
        redis_cached.append(None)
        redis_cached[3] = ['tbaker']
        self.assertEqual(redis_cached[3], ['tbaker'])

        # __delitem__
        del redis_cached[-1]
        self.assertEqual(len(redis_cached), 3)

        del redis_cached[0]
        self.assertEqual(len(redis_cached), 2)
        self.assertEqual(redis_cached.cache[0], ['ptroughton'])

        # insert
        redis_cached.insert(0, ['whartnell'])
        self.assertEqual(len(redis_cached), 3)
        self.assertEqual(redis_cached.cache[0], ['whartnell'])
        self.assertEqual(redis_cached.cache[1], ['ptroughton'])

        # index
        redis_cached.append([None])
        redis_cached.append([None])
        redis_cached.append([None])
        redis_cached[3][0] = 'tbaker'
        redis_cached[4][0] = 'tbaker'
        redis_cached[5][0] = 'pdavison'

        self.assertEqual(redis_cached.index(['tbaker']), 3)
        self.assertEqual(redis_cached.index(['tbaker'], 4), 4)
        with self.assertRaises(ValueError):
                redis_cached.index(['tbaker'], 0, 2)

        # remove (forces sync)
        redis_cached.remove(['tbaker'])
        self.assertEqual(len(redis_cached), 5)
        self.assertEqual(list(redis_cached)[3:], [['tbaker'], ['pdavison']])

        # extend
        redis_cached.extend([['cbaker'], ['smccoy'], ['pmcgann', 'jhurt']])
        self.assertEqual(len(redis_cached), 8)
        self.assertEqual(redis_cached[4], ['pdavison'])
        self.assertEqual(redis_cached[5], ['cbaker'])
        self.assertEqual(redis_cached[6], ['smccoy'])
        self.assertEqual(redis_cached[7], ['pmcgann', 'jhurt'])

        # pop
        redis_cached.insert(0, ['morbius'])
        self.assertEqual(redis_cached.pop(0), ['morbius'])
        self.assertEqual(redis_cached[0], ['whartnell'])
        self.assertEqual(len(redis_cached), 8)

        redis_cached.append(['ceccleston'])
        self.assertEqual(redis_cached.pop(), ['ceccleston'])
        self.assertEqual(len(redis_cached), 8)

    def test_with(self):
        with self.create_list(writeback=True) as redis_cached:
            redis_cached.append({'one': 1})
            redis_cached[0]['one'] = 2
            self.assertEqual(list(redis_cached._data())[0], {'one': 1})

        self.assertEqual(list(redis_cached._data())[0], {'one': 2})

if __name__ == '__main__':
    unittest.main()
