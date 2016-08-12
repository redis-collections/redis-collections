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
        for init_args in [
            [[0, 1, 2, 3]],  # List from list
            ['0123'],  # List from str
            [b'0123'],  # List from bytes
            [],  # Empty List
        ]:
            redis_list = self.create_list(*init_args)
            python_list = list(*init_args)
            self.assertEqual(list(redis_list), python_list)

    def test_contains(self):
        data = (0, 1, 2, 3)
        redis_list = self.create_list(data)
        python_list = list(data)

        for L in (redis_list, python_list):
            self.assertIn(0, L)
            self.assertIn(3, L)
            self.assertNotIn(4, L)

    def test_get_set_del_index(self):
        data = ('zero', 'one', 'two', 'three')
        for i in (0, 1, 2, 3, -1, -2, -3, -4):
            redis_list = self.create_list(data)
            redis_cached = self.create_list(data, writeback=True)
            python_list = list(data)

            # Get the values
            self.assertEqual(redis_list[i], python_list[i], i)
            self.assertEqual(redis_cached[i], python_list[i], i)

            # Set the values and get them again
            redis_list[i] = i
            redis_cached[i] = i
            python_list[i] = i
            self.assertEqual(redis_list[i], python_list[i], i)
            self.assertEqual(redis_cached[i], python_list[i], i)

            # Delete the values
            del redis_list[i]
            del python_list[i]
            del redis_cached[i]

            self.assertEqual(list(redis_list), python_list, i)
            self.assertEqual(list(redis_cached), python_list, i)

        for L in (redis_list, redis_cached, python_list):
            self.assertRaises(IndexError, L.__getitem__, 4)
            self.assertRaises(IndexError, L.__getitem__, -5)

            with self.assertRaises(IndexError):
                L[100] = 'x'

    def test_get_del_slice(self):
        data = (0, 1, 2, 3, 4, 5)
        for slice_args in [
            (None, None, None),
            (0, None, None),
            (0, 0, None),
            (0, 5, None),
            (0, 6, None),
            (None, 1, None),
            (0, 2, None),
            (0, -3, None),
            (0, 4, None),
            (0, -1, None),
            (0, 6, None),
            (1, None, None),
            (2, 6, None),
            (3, 6, None),
            (-2, None, None),
            (-1, None, None),
            (1, -1, None),
            (2, -2, None),
            (3, -3, None),
            (-5, 5, None),
            (None, None, -1),
            (None, None, 1),
            (None, None, 2),
            (None, None, 3),
            (1, -1, 2),
            (5, 1, -1),
            (5, 1, -2),
        ]:
            slice_obj = slice(*slice_args)

            redis_list = self.create_list(data)
            redis_cached = self.create_list(data, writeback=True)
            python_list = list(data)

            self.assertEqual(redis_list[slice_obj], python_list[slice_obj])
            self.assertEqual(redis_cached[slice_obj], python_list[slice_obj])

            del redis_list[slice_obj]
            del redis_cached[slice_obj]
            del python_list[slice_obj]

            self.assertEqual(list(redis_list), python_list, slice_args)
            self.assertEqual(list(redis_cached), python_list, slice_args)

    def test_iter(self):
        data = ('zero', 'one', 'two', 'three')
        redis_list_iter = iter(self.create_list(data))
        redis_cached_iter = iter(self.create_list(data, writeback=True))
        python_iter = iter(list(data))
        for v in data:
            self.assertEqual(next(redis_list_iter), v)
            self.assertEqual(next(redis_cached_iter), v)
            self.assertEqual(next(python_iter), v)

    def test_len(self):
        for data, expected in [(tuple(), 0), ((0,), 1), ((0, 1,), 2)]:
            redis_list = self.create_list(data)
            redis_cached = self.create_list(data, writeback=True)
            python_list = list(data)

            self.assertEqual(len(redis_list), expected)
            self.assertEqual(len(redis_cached), expected)
            self.assertEqual(len(python_list), expected)

    def test_reversed(self):
        data = ('zero', 'one', 'two', 'three')
        redis_list = self.create_list(data)
        redis_cached = self.create_list(data, writeback=True)
        python_list = list(data)

        for L in (redis_list, redis_cached, python_list):
            self.assertEqual(list(reversed(L)), list(reversed(data)))
            self.assertEqual(list(reversed(L)), list(reversed(data)))

            L.reverse()
            self.assertEqual(list(L), list(reversed(data)))

    def test_set_slice(self):
        data = ('a', 'b', 'c', 'd', 'e', 'f')
        for init, kwargs in (
            (self.create_list, {}),
            (self.create_list, {'writeback': True}),
            (list, {}),
        ):
            L = init(data, **kwargs)
            L[:1] = ['A', 'B']
            self.assertEqual(list(L), ['A', 'B', 'b', 'c', 'd', 'e', 'f'])

            L[2:-2] = ['C', 'D']
            self.assertEqual(list(L), ['A', 'B', 'C', 'D', 'e', 'f'])

            L[4:100] = ['E', 'F', 'G', 'H']
            self.assertEqual(list(L), ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'])

            L[::2] = ['x', 'x', 'x', 'x']
            self.assertEqual(list(L), ['x', 'B', 'x', 'D', 'x', 'F', 'x', 'H'])

            L[6:1:-2] = ['y', 'y', 'y']
            self.assertEqual(list(L), ['x', 'B', 'y', 'D', 'y', 'F', 'y', 'H'])

            # Sequence length doesn't match
            with self.assertRaises(ValueError):
                L[6:1:-2] = ['y', 'y', 'y', 'y']

            # Zero step
            with self.assertRaises(ValueError):
                L[::0] = []

    def test_append(self):
        for init, kwargs in (
            (self.create_list, {}),
            (self.create_list, {'writeback': True}),
            (list, {}),
        ):
            L = init(**kwargs)
            L.append('zero')
            L.append('one')
            L.append('two')

            self.assertEqual(list(L), ['zero', 'one', 'two'])

    def test_clear(self):
        data = ('zero', 'one', 'two', 'three')
        redis_list = self.create_list(data)
        redis_cached = self.create_list(data, writeback=True)

        # Python 2.x lists don't have a clear method
        for L in (redis_list, redis_cached):
            L[0] = 'Zero'
            L.clear()
            self.assertEqual(list(L), [])

            with self.assertRaises(IndexError):
                L[0]

    def test_copy(self):
        data = ('zero', 'one', 'two', 'three')

        redis_list = self.create_list(data)
        redis_list[0] = 'Zero'
        new_list = redis_list.copy(redis=redis_list.redis)
        self.assertEqual(list(new_list), list(redis_list))
        self.assertTrue(new_list.redis is redis_list.redis)
        self.assertFalse(new_list.writeback)

        redis_cached = self.create_list(data, writeback=True)
        redis_cached[0] = 'ZERO'
        new_cached = redis_cached.copy()
        self.assertEqual(list(new_cached), list(redis_cached))
        self.assertTrue(new_cached.redis is redis_cached.redis)
        self.assertTrue(new_cached.writeback)

    def test_count(self):
        data = ('a', 'b', 'b', 'c', 'c', 'c', None)
        redis_list = self.create_list(data)
        redis_cached = self.create_list(data, writeback=True)
        python_list = list(data)

        for L in (redis_list, redis_cached, python_list):
            self.assertEqual(L.count('a'), 1)
            self.assertEqual(L.count('b'), 2)
            self.assertEqual(L.count('c'), 3)
            self.assertEqual(L.count(None), 1)
            self.assertEqual(L.count('A'), 0)

    def test_extend(self):
        data = (0, 1,)
        redis_cached = self.create_list(data, writeback=True)
        redis_list = self.create_list(data)
        python_list = list(data)

        for L in (redis_list, redis_cached, python_list):
            L.extend([2, 3])
            self.assertEqual(list(L), [0, 1, 2, 3])

            L += [4, 5]
            self.assertEqual(list(L), [0, 1, 2, 3, 4, 5])

        redis_list.extend(redis_cached)
        self.assertEqual(list(redis_list), [0, 1, 2, 3, 4, 5] * 2)

    def test_index(self):
        data = ('a', 'b', 'b', 'c', 'c', 'c', None)
        redis_list = self.create_list(data)
        redis_cached = self.create_list(data, writeback=True)
        python_list = list(data)

        for L in (redis_list, redis_cached, python_list):
            self.assertEqual(L.index('a'), 0)
            self.assertEqual(L.index('b'), 1)
            self.assertEqual(L.index('b', 1), 1)
            self.assertEqual(L.index('b', 2), 2)
            self.assertRaises(ValueError, L.index, 'b', 3)
            self.assertEqual(L.index('c', 4, 5), 4)

    def test_insert(self):
        redis_list = self.create_list()
        redis_cached = self.create_list(writeback=True)
        python_list = list()

        for L in (redis_list, redis_cached, python_list):
            L.insert(0, 'b')
            L.insert(0, 'a')
            self.assertEqual(list(L), ['a', 'b'])

            L.insert(2, 'd')
            L.insert(-1, 'c')
            self.assertEqual(list(L), ['a', 'b', 'c', 'd'])

            L.insert(-2, 'x')
            self.assertEqual(list(L), ['a', 'b', 'x', 'c', 'd'])

            L.insert(10, '!')
            self.assertEqual(list(L), ['a', 'b', 'x', 'c', 'd', '!'])

    def test_pop(self):
        data = ('zero', 'one', 'two', 'three', 'four', 'five')
        redis_list = self.create_list(data)
        redis_cached = self.create_list(data, writeback=True)
        python_list = list(data)

        for L in (redis_list, redis_cached, python_list):
            self.assertEqual(L.pop(), 'five')
            self.assertEqual(list(L), list(data[:-1]))

            self.assertEqual(L.pop(0), 'zero')
            self.assertEqual(list(L), list(data[1:-1]))

            self.assertEqual(L.pop(-1), 'four')
            self.assertEqual(list(L), list(data[1:-2]))

            self.assertEqual(L.pop(1), 'two')
            self.assertEqual(list(L), ['one', 'three'])

            self.assertRaises(IndexError, L.pop, 100)
            self.assertEqual(L.pop(), 'three')
            self.assertEqual(L.pop(), 'one')
            self.assertRaises(IndexError, L.pop)
            self.assertRaises(IndexError, L.pop, 0)

    def test_remove(self):
        data = ('a', 'b', 'b', 'c', 'c', 'c', None)
        redis_list = self.create_list(data)
        redis_cached = self.create_list(data, writeback=True)
        python_list = list(data)

        for L in (redis_list, redis_cached, python_list):
            L.remove(None)
            self.assertEqual(list(L), ['a', 'b', 'b', 'c', 'c', 'c'])

            L.remove('a')
            self.assertEqual(list(L), ['b', 'b', 'c', 'c', 'c'])

            L.remove('b')
            self.assertEqual(list(L), ['b', 'c', 'c', 'c'])

            L.remove('c')
            self.assertEqual(list(L), ['b', 'c', 'c'])

            self.assertRaises(ValueError, L.remove, 'd')

    def test_sort(self):
        data = ('zero', 'one', 'two', 'three')
        redis_list = self.create_list(data)
        redis_cached = self.create_list(data, writeback=True)
        python_list = list(data)

        for L in (redis_list, redis_cached, python_list):
            L.sort()
            self.assertEqual(list(L), ['one', 'three', 'two', 'zero'])

            L.sort(key=lambda x: x[::-1])
            self.assertEqual(list(L), ['three', 'one', 'zero', 'two'])

            L.sort(reverse=True)
            self.assertEqual(list(L), ['zero', 'two', 'three', 'one'])

            L.sort(key=lambda x: x[::-1], reverse=True)
            self.assertEqual(list(L), ['two', 'zero', 'one', 'three'])

    def test_add(self):
        data = (0, 1, 2, 3)
        redis_list = self.create_list(data)
        redis_cached = self.create_list(data, writeback=True)
        python_list = list(data)

        for L in (redis_list, redis_cached, python_list):
            self.assertEqual(L + ['x', 'y'], [0, 1, 2, 3, 'x', 'y'])

    def test_mul(self):
        data = (0, 1)
        redis_list = self.create_list(data)
        redis_cached = self.create_list(data, writeback=True)
        python_list = list(data)

        for L in (redis_list, redis_cached, python_list):
            self.assertEqual(L * 2, [0, 1, 0, 1])
            self.assertEqual(L * 0, [])
            self.assertEqual(L * -1, [])

            self.assertEqual(2 * L, [0, 1, 0, 1])
            self.assertEqual(0 * L, [])
            self.assertEqual(-1 * L, [])

            with self.assertRaises(TypeError):
                L * None

    def test_imul(self):
        data = (0, 1)
        for init, kwargs in (
            (self.create_list, {}),
            (self.create_list, {'writeback': True}),
            (list, {}),
        ):
            L = init(data, **kwargs)

            L *= 1
            self.assertEqual(list(L), [0, 1])

            L *= 2
            self.assertEqual(list(L), [0, 1] * 2)

            L *= 0
            self.assertEqual(list(L), [])

            with self.assertRaises(TypeError):
                L *= None

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
        # self.assertEqual(redis_cached.get(0), ['whartnell'])

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
