#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, unicode_literals

from decimal import Decimal
from fractions import Fraction
import unittest
import sys

from redis_collections import Set

from .base import RedisTestCase


PYTHON_VERSION = (sys.version_info[0], sys.version_info[1])


class SetTest(RedisTestCase):

    def create_set(self, *args, **kwargs):
        kwargs['redis'] = self.redis
        return Set(*args, **kwargs)

    def test_init(self):
        for init in (self.create_set, set):
            s = init([1, 2, 3])
            self.assertEqual(sorted(s), [1, 2, 3])

            s = init('abc')
            self.assertEqual(sorted(s), ['a', 'b', 'c'])

            s = init('antananarivo')
            self.assertEqual(sorted(s), ['a', 'i', 'n', 'o', 'r', 't', 'v'])

            s = init()
            self.assertEqual(sorted(s), [])

    def test_len(self):
        for init in (self.create_set, set):
            s = init([1, 2, 3, 3])
            self.assertEqual(len(s), 3)

    def test_in(self):
        for init in (self.create_set, set):
            s = init([1, 2, 3, 3])
            self.assertIn(1, s)
            self.assertNotIn(4, s)

    def test_equal(self):
        for init in (self.create_set, set):
            s_1 = init([1, 2, 3, 3])
            s_2 = init([4, 5])
            s_3 = {4, 5}
            self.assertNotEqual(s_1, s_3)
            self.assertNotEqual(s_1, s_3)
            self.assertEqual(s_2, s_3)
            self.assertEqual(s_3, s_3)

    def test_disjoint(self):
        for init in (self.create_set, set):
            s_1 = init([1, 2, 3, 3])
            s_2 = init([4, 5])
            s_3 = {3, 4, 5}
            s_4 = [4, 5]

            self.assertTrue(s_1.isdisjoint(s_2))
            self.assertFalse(s_1.isdisjoint(s_3))
            self.assertTrue(s_1.isdisjoint(s_4))
            self.assertRaises(TypeError, s_1.isdisjoint, None)

    def test_eq_le_lt_issubset(self):
        for init in (self.create_set, set,):
            s_1 = init([1, 2])
            s_2 = init([1, 2, 3, 4])
            s_3 = {1, 2, 3, 4}
            s_4 = {1, 2}
            s_5 = [1, 2, 3, 4]

            self.assertTrue(s_1.issubset(s_2))
            self.assertFalse(s_1 == s_2)
            self.assertTrue(s_1 <= s_2)
            self.assertTrue(s_1 < s_2)

            self.assertTrue(s_1.issubset(s_3))
            self.assertFalse(s_1 == s_3)
            self.assertTrue(s_1 <= s_3)
            self.assertTrue(s_1 < s_3)

            self.assertTrue(s_1.issubset(s_4))
            self.assertTrue(s_1 == s_4)
            self.assertTrue(s_1 <= s_4)
            self.assertFalse(s_1 < s_4)

            self.assertTrue(s_1.issubset(s_5))
            if PYTHON_VERSION >= (3, 4):
                self.assertRaises(TypeError, lambda: s_1 <= s_5)

            self.assertRaises(TypeError, s_1.issubset, None)

    def test_superset(self):
        for init in (self.create_set, set):
            s_1 = init([1, 2, 3, 4])
            s_2 = init([1, 2])
            s_3 = init([1, 2, 3, 4, 5])
            s_4 = {1, 2}
            s_5 = {1, 2, 3, 4}
            s_6 = [1, 2]

            self.assertTrue(s_1.issuperset(s_2))
            self.assertTrue(s_1 >= s_2)
            self.assertTrue(s_1 > s_2)

            self.assertFalse(s_1.issuperset(s_3))
            self.assertFalse(s_1 >= s_3)
            self.assertFalse(s_1 > s_3)

            self.assertTrue(s_1.issuperset(s_4))
            self.assertTrue(s_1 >= s_4)
            self.assertTrue(s_1 > s_4)

            self.assertTrue(s_1.issuperset(s_5))
            self.assertTrue(s_1 >= s_5)
            self.assertFalse(s_1 > s_5)

            self.assertTrue(s_1.issuperset(s_6))
            if PYTHON_VERSION >= (3, 4):
                self.assertRaises(TypeError, lambda: s_1 >= s_6)

            self.assertRaises(TypeError, s_1.issuperset, None)

    def test_union(self):
        for init in (self.create_set, set):
            s_1 = init([1, 2])
            s_2 = init([2, 3, 4])
            s_3 = {2, 3, 4}
            s_4 = [2, 3, 4]

            self.assertEqual(sorted(s_1.union(s_2)), [1, 2, 3, 4])
            self.assertEqual(sorted(s_1 | s_2), [1, 2, 3, 4])
            self.assertEqual(sorted(s_2 | s_1), [1, 2, 3, 4])

            self.assertEqual(sorted(s_1.union(s_3)), [1, 2, 3, 4])
            self.assertEqual(sorted(s_1 | s_3), [1, 2, 3, 4])
            self.assertEqual(sorted(s_3 | s_1), [1, 2, 3, 4])

            self.assertEqual(sorted(s_1.union(s_4)), [1, 2, 3, 4])
            self.assertRaises(TypeError, lambda: s_1 | s_4)
            self.assertRaises(TypeError, lambda: s_4 | s_1)

    def test_intersection(self):
        for init in (self.create_set, set):
            s_1 = init([1, 2, 3])
            s_2 = init([2, 3, 4])
            s_3 = {2, 3, 4}
            s_4 = [2, 3, 4]

            self.assertEqual(sorted(s_1.intersection(s_2)), [2, 3])
            self.assertEqual(sorted(s_1 & s_2), [2, 3])
            self.assertEqual(sorted(s_2 & s_1), [2, 3])

            self.assertEqual(sorted(s_1.intersection(s_3)), [2, 3])
            self.assertEqual(sorted(s_1 & s_3), [2, 3])
            self.assertEqual(sorted(s_3 & s_1), [2, 3])

            self.assertEqual(sorted(s_1.intersection(s_4)), [2, 3])
            self.assertRaises(TypeError, lambda: s_1 & s_4)

    def test_difference(self):
        for init in (self.create_set, set):
            s_1 = init([1, 2, 3, 4])
            s_2 = init([3, 4])
            s_3 = {3, 4}
            s_4 = [3, 4]

            self.assertEqual(sorted(s_1.difference(s_2)), [1, 2])
            self.assertEqual(sorted(s_1 - s_2), [1, 2])
            self.assertEqual(sorted(s_2 - s_1), [])

            self.assertEqual(sorted(s_1.difference(s_3)), [1, 2])
            self.assertEqual(sorted(s_1 - s_3), [1, 2])
            self.assertEqual(sorted(s_3 - s_1), [])

            self.assertEqual(sorted(s_1.difference(s_4)), [1, 2])
            self.assertRaises(TypeError, lambda: s_1 - s_4)

    def test_symmetric_difference(self):
        for init in (self.create_set, set):
            s_1 = init([1, 2, 3, 4])
            s_2 = init([3, 4, 5, 6])
            s_3 = {3, 4, 5, 6}
            s_4 = [3, 4, 5, 6]

            self.assertEqual(
                sorted(s_1.symmetric_difference(s_2)), [1, 2, 5, 6]
            )
            self.assertEqual(sorted(s_1 ^ s_2), [1, 2, 5, 6])
            self.assertEqual(sorted(s_2 ^ s_1), [1, 2, 5, 6])

            self.assertEqual(
                sorted(s_1.symmetric_difference(s_3)), [1, 2, 5, 6]
            )
            self.assertEqual(sorted(s_1 ^ s_3), [1, 2, 5, 6])
            self.assertEqual(sorted(s_3 ^ s_1), [1, 2, 5, 6])

            self.assertEqual(
                sorted(s_1.symmetric_difference(s_4)), [1, 2, 5, 6]
            )
            self.assertRaises(TypeError, lambda: s_1 ^ s_4)

    def test_copy(self):
        for init in (self.create_set, set):
            s_1 = init('abc')
            s_2 = s_1.copy()
            self.assertEqual(s_1.__class__, s_2.__class__)
            self.assertEqual(sorted(s_1), sorted(s_2))

    def test_update(self):
        for init in (self.create_set, set):
            s_1 = init([0, 1])
            s_2 = init([1, 2])
            s_3 = init([2, 3])
            s_4 = {3, 4}
            s_5 = {4, 5}
            s_6 = [5, 6]
            s_7 = [6, 7]

            s_1.update(s_2)
            self.assertEqual(sorted(s_1), list(range(3)))

            s_1 |= s_3
            self.assertEqual(sorted(s_1), list(range(4)))

            s_1.update(s_4)
            self.assertEqual(sorted(s_1), list(range(5)))

            s_1 |= s_5
            self.assertEqual(sorted(s_1), list(range(6)))

            s_1.update(s_6)
            self.assertEqual(sorted(s_1), list(range(7)))

            with self.assertRaises(TypeError):
                s_1 |= s_7

    def test_intersection_update(self):
        for init in (self.create_set, set):
            s_1 = init(range(8))
            s_2 = init(range(7))
            s_3 = init(range(6))
            s_4 = set(range(5))
            s_5 = set(range(4))
            s_6 = list(range(3))
            s_7 = list(range(2))

            s_1.intersection_update(s_2)
            self.assertEqual(sorted(s_1), list(range(7)))

            s_1 &= s_3
            self.assertEqual(sorted(s_1), list(range(6)))

            s_1.intersection_update(s_4)
            self.assertEqual(sorted(s_1), list(range(5)))

            s_1 &= s_5
            self.assertEqual(sorted(s_1), list(range(4)))

            s_1.intersection_update(s_6)
            self.assertEqual(sorted(s_1), list(range(3)))

            with self.assertRaises(TypeError):
                s_1 &= s_7

    def test_difference_update(self):
        for init in (self.create_set, set):
            s_1 = init(range(8))
            s_2 = init(range(2))
            s_3 = init(range(3))
            s_4 = set(range(4))
            s_5 = set(range(5))
            s_6 = list(range(6))
            s_7 = list(range(7))

            s_1.difference_update(s_2)
            self.assertEqual(sorted(s_1), list(range(2, 8)))

            s_1 -= s_3
            self.assertEqual(sorted(s_1), list(range(3, 8)))

            s_1.difference_update(s_4)
            self.assertEqual(sorted(s_1), list(range(4, 8)))

            s_1 -= s_5
            self.assertEqual(sorted(s_1), list(range(5, 8)))

            s_1.difference_update(s_6)
            self.assertEqual(sorted(s_1), list(range(6, 8)))

            with self.assertRaises(TypeError):
                s_1 -= s_7

    def test_symmetric_difference_update(self):
        for init in (self.create_set, set):
            s_1 = init('ab')
            s_2 = init('bc')
            s_3 = init('cd')
            s_4 = set('de')
            s_5 = set('ef')
            s_6 = 'fg'
            s_7 = 'gh'

            s_1.symmetric_difference_update(s_2)
            self.assertEqual(''.join(sorted(s_1)), 'ac')

            s_1 ^= s_3
            self.assertEqual(''.join(sorted(s_1)), 'ad')

            s_1.symmetric_difference_update(s_4)
            self.assertEqual(''.join(sorted(s_1)), 'ae')

            s_1 ^= s_5
            self.assertEqual(''.join(sorted(s_1)), 'af')

            s_1.symmetric_difference_update(s_6)
            self.assertEqual(''.join(sorted(s_1)), 'ag')

            with self.assertRaises(TypeError):
                s_1 ^= s_7

    def test_add(self):
        for init in (self.create_set, set):
            s = init('ab')
            s.add('b')
            s.add('c')
            self.assertEqual(sorted(s), ['a', 'b', 'c'])

            self.assertRaises(TypeError, s.add, dict())

    def test_remove_discard(self):
        for init in (self.create_set, set):
            s = init('cdab')
            self.assertRaises(KeyError, s.remove, 'x')
            s.remove('b')
            self.assertEqual(sorted(s), ['a', 'c', 'd'])
            s.discard('x')
            s.discard('a')
            self.assertEqual(sorted(s), ['c', 'd'])

    def test_pop(self):
        for init in (self.create_set, set):
            s = init('a')
            self.assertEqual(s.pop(), 'a')
            self.assertEqual(sorted(s), [])
            self.assertRaises(KeyError, s.pop)

    def test_random_sample(self):
        s = self.create_set('a')
        self.assertEqual(s.random_sample(0), [])
        self.assertEqual(s.random_sample(), ['a'])

        redis_version = self.redis.info()['redis_version']
        redis_version = [int(x) for x in redis_version.split('.')]
        major_ver, minor_ver, _ = redis_version
        if (major_ver > 2) or (major_ver >= 2 and minor_ver >= 6):
            s = self.create_set('ab')
            self.assertEqual(sorted(s.random_sample(2)), ['a', 'b'])

    def test_add_unicode(self):
        for init in (self.create_set, set):
            s = init()
            elem = 'ěščřžýáíéůú\U0001F4A9'
            s.add(elem)
            self.assertEqual(sorted(s), [elem])

    def test_add_equal_hashes(self):
        redis_set = Set()
        python_set = set()
        for value in [
            1.0,
            1,
            complex(1.0, 0.0),
            Decimal(1.0),
            Fraction(2, 2),
            u'a',
            b'a',
            'a',
        ]:
            redis_set.add(value)
            python_set.add(value)

            self.assertEqual(len(redis_set), len(python_set))

            self.assertIn(value, redis_set)
            self.assertIn(value, python_set)

            redis_values = []
            while redis_set:
                redis_values.append(redis_set.pop())

            python_values = []
            while python_set:
                python_values.append(python_set.pop())

            self.assertEqual(sorted(redis_values), sorted(python_values))

    def test_clear(self):
        for init in (self.create_set, set):
            s = self.create_set('abcdefg')
            s.clear()
            self.assertEqual(sorted(s), [])


class _Set(Set):
    pass


class SubClassTest(SetTest):
    """Subclasses should be working properly, too"""

    def create_set(self, *args, **kwargs):
        kwargs['redis'] = self.redis
        return _Set(*args, **kwargs)

    def test_copy(self):
        s1 = self.create_set('abc')
        s2 = s1.copy()
        self.assertEqual(s2.__class__, _Set)
        self.assertEqual(sorted(s1),
                         sorted(s2))


if __name__ == '__main__':
    unittest.main()
