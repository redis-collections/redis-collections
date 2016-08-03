#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, unicode_literals

import unittest

from redis_collections import Set

from .base import RedisTestCase


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
            self.assertEqual(1 in s, True)
            self.assertEqual(42 in s, False)
            self.assertEqual(1 not in s, False)
            self.assertEqual(42 not in s, True)

    def test_equal(self):
        for init in (self.create_set, set):
            s_1 = init([1, 2, 3, 3])
            s_2 = init([4, 5])
            s_3 = init([4, 5])
            self.assertNotEqual(s_1, s_3)
            self.assertNotEqual(s_1, s_3)
            self.assertEqual(s_2, s_3)
            self.assertEqual(s_3, s_3)

    def test_disjoint(self):
        for init in (self.create_set, set):
            s_1 = init([1, 2, 3, 3])
            s_2 = init([4, 5])
            self.assertTrue(s_1.isdisjoint(s_2))

    def test_subset(self):
        for init in (self.create_set, set):
            s_1 = init([1, 2, 3, 3])
            s_2 = init([4, 5])
            self.assertFalse(s_2.issubset(s_1))

            s_2 = init([3, 2])
            self.assertTrue(s_2.issubset(s_1))
            self.assertTrue(s_2 <= s_1)
            self.assertTrue(s_2 < s_1)

            s_2 = init([1, 2, 3, 3])
            self.assertFalse(s_2 < s_1)

    def test_superset(self):
        for init in (self.create_set, set):
            s_1 = init([1, 2, 3, 3])
            s_2 = init([4, 5])
            self.assertFalse(s_2.issuperset(s_1))

            s_2 = init([3, 2])
            self.assertTrue(s_1.issuperset(s_2))
            self.assertTrue(s_1 >= s_2)
            self.assertTrue(s_1 > s_2)

            s_2 = init([1, 2, 3, 3])
            self.assertFalse(s_1 > s_2)

    def test_union(self):
        for init in (self.create_set, set):
            s_1 = init([1, 2, 3, 3])
            s_2 = init([4, 5])
            s_3 = set([6])
            l = [6]
            self.assertEqual(sorted(s_1 | s_2), [1, 2, 3, 4, 5])
            self.assertEqual(sorted(s_1.union(s_2)), [1, 2, 3, 4, 5])
            self.assertEqual(sorted(s_1 | s_2 | s_3), [1, 2, 3, 4, 5, 6])
            self.assertEqual(sorted(s_1.union(s_2, s_3)), [1, 2, 3, 4, 5, 6])
            self.assertRaises(TypeError, lambda: s_1 | s_2 | l)
            self.assertEqual(sorted(s_1.union(s_2, l)), [1, 2, 3, 4, 5, 6])

    def test_intersection(self):
        for init in (self.create_set, set):
            s_1 = init([1, 2, 3, 3])
            s_2 = init([3, 4, 5])
            s_3 = set([6])
            l = [6]
            self.assertEqual(sorted(s_1 & s_2), [3])
            self.assertEqual(sorted(s_1.intersection(s_2)), [3])
            self.assertEqual(sorted(s_1 & s_2 & s_3), [])
            self.assertEqual(sorted(s_1.intersection(s_2, s_3)), [])
            self.assertRaises(TypeError, lambda: s_1 & s_2 & l)
            self.assertEqual(sorted(s_1.intersection(s_2, l)), [])
            self.assertEqual(sorted(s_3 & s_2), [])

    def test_difference(self):
        for init in (self.create_set, set):
            s_1 = init([1, 2, 3, 3])
            s_2 = init([3, 4, 5])
            s_3 = set([6])
            l = [6]
            self.assertEqual(sorted(s_1 - s_2), [1, 2])
            self.assertEqual(sorted(s_1.difference(s_2)), [1, 2])
            self.assertEqual(sorted(s_1 - s_2 - s_3), [1, 2])
            self.assertEqual(sorted(s_1.difference(s_2, s_3)), [1, 2])
            self.assertRaises(TypeError, lambda: s_1 - s_2 - l)
            self.assertEqual(sorted(s_1.difference(s_2, l)), [1, 2])
            self.assertEqual(sorted(s_3 - s_1), [6])

    def test_symmetric_difference(self):
        for init in (self.create_set, set):
            s_1 = init([1, 2, 3, 3])
            s_2 = init([3, 4, 5])
            s_3 = set([6])
            l = [6]
            self.assertEqual(sorted(s_1 ^ s_2), [1, 2, 4, 5])
            self.assertEqual(
                sorted(s_1.symmetric_difference(s_2)), [1, 2, 4, 5]
            )
            self.assertEqual(sorted(s_1 ^ s_2 ^ s_3), [1, 2, 4, 5, 6])
            self.assertRaises(TypeError, lambda: s_1 ^ s_2 ^ l)
            self.assertEqual(sorted(s_3 ^ s_1 ^ s_2), [1, 2, 4, 5, 6])

    def test_copy(self):
        for init in (self.create_set, set):
            s_1 = init('abc')
            s_2 = s_1.copy()
            self.assertEqual(s_1.__class__, s_2.__class__)
            self.assertEqual(sorted(s_1), sorted(s_2))

    def test_result_type(self):
        for init in (self.create_set, set):
            s_1 = init('ab')
            s_2 = set('bc')
            s_3 = s_1 | s_2
            s4 = s_2 | s_1
            self.assertEqual(s_3.__class__, s_1.__class__)
            self.assertEqual(s4.__class__, s_2.__class__)

    def test_update(self):
        for init in (self.create_set, set):
            s_1 = init('ab')
            s_2 = frozenset('bc')
            st = 'cd'
            s_1 |= s_2
            self.assertEqual(sorted(s_1), ['a', 'b', 'c'])
            s_1.update(s_2, st)
            self.assertEqual(sorted(s_1), ['a', 'b', 'c', 'd'])

    def test_intersection_update(self):
        for init in (self.create_set, set):
            s_1 = init('ab')
            s_2 = frozenset('bc')
            st = 'cd'
            s_1 &= s_2
            self.assertEqual(sorted(s_1), ['b'])
            s_1.intersection_update(s_2, st)
            self.assertEqual(sorted(s_1), [])

    def test_difference_update(self):
        for init in (self.create_set, set):
            s_1 = init('ab')
            s_2 = frozenset('bc')
            s_3 = 'cd'
            s_1 -= s_2
            self.assertEqual(sorted(s_1), ['a'])
            s_1.difference_update(s_2, s_3)
            self.assertEqual(sorted(s_1), ['a'])

    def test_symmetric_difference_update(self):
        for init in (self.create_set, set):
            s_1 = init('ab')
            s_2 = frozenset('bc')
            s_3 = 'cd'
            s_1 ^= s_2
            self.assertEqual(sorted(s_1), ['a', 'c'])
            s_1.symmetric_difference_update(s_3)
            self.assertEqual(sorted(s_1), ['a', 'd'])

    def test_add(self):
        s = self.create_set('ab')
        s.add('c')
        self.assertEqual(sorted(s), ['a', 'b', 'c'])

        # Returning True or False after addition isn't something the native
        # Python `set` does
        self.assertFalse(s.add('c'))
        self.assertTrue(s.add('d'))

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
        self.assertEqual(s.random_sample(), ['a'])

        redis_version = self.redis.info()['redis_version']
        redis_version = [int(x) for x in redis_version.split('.')]
        major_ver, minor_ver, _ = redis_version

        if major_ver >= 2 and minor_ver >= 6:
            s = self.create_set('ab')
            self.assertEqual(sorted(s.random_sample(2)), ['a', 'b'])

    def test_add_unicode(self):
        for init in (self.create_set, set):
            s = init()
            elem = 'ěščřžýáíéůú\U0001F4A9'
            s.add(elem)
            self.assertEqual(sorted(s), [elem])

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
