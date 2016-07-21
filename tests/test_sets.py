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
        s = self.create_set([1, 2, 3])
        self.assertEqual(sorted(s), [1, 2, 3])
        s = self.create_set('abc')
        self.assertEqual(sorted(s), ['a', 'b', 'c'])
        s = self.create_set('antananarivo')
        self.assertEqual(sorted(s), ['a', 'i', 'n', 'o', 'r', 't', 'v'])
        s = self.create_set()
        self.assertEqual(sorted(s), [])

    def test_len(self):
        s = self.create_set([1, 2, 3, 3])
        self.assertEqual(len(s), 3)

    def test_in(self):
        s = self.create_set([1, 2, 3, 3])
        self.assertEqual(1 in s, True)
        self.assertEqual(42 in s, False)
        self.assertEqual(1 not in s, False)
        self.assertEqual(42 not in s, True)

    def test_equal(self):
        s1 = self.create_set([1, 2, 3, 3])
        s2 = self.create_set([4, 5])
        s3 = self.create_set([4, 5])
        self.assertFalse(s1 == s2)
        self.assertTrue(s1 != s3)
        self.assertTrue(s2 == s3)
        self.assertTrue(s3 == s3)

    def test_disjoint(self):
        s1 = self.create_set([1, 2, 3, 3])
        s2 = self.create_set([4, 5])
        self.assertTrue(s1.isdisjoint(s2))

    def test_subset(self):
        s1 = self.create_set([1, 2, 3, 3])
        s2 = self.create_set([4, 5])
        self.assertFalse(s2.issubset(s1))
        s2 = self.create_set([3, 2])
        self.assertTrue(s2.issubset(s1))
        self.assertTrue(s2 <= s1)
        self.assertTrue(s2 < s1)
        s2 = self.create_set([1, 2, 3, 3])
        self.assertFalse(s2 < s1)

    def test_superset(self):
        s1 = self.create_set([1, 2, 3, 3])
        s2 = self.create_set([4, 5])
        self.assertFalse(s2.issuperset(s1))
        s2 = self.create_set([3, 2])
        self.assertTrue(s1.issuperset(s2))
        self.assertTrue(s1 >= s2)
        self.assertTrue(s1 > s2)
        s2 = self.create_set([1, 2, 3, 3])
        self.assertFalse(s1 > s2)

    def test_union(self):
        s1 = self.create_set([1, 2, 3, 3])
        s2 = self.create_set([4, 5])
        s3 = set([6])
        l = [6]
        self.assertEqual(sorted(s1 | s2), [1, 2, 3, 4, 5])
        self.assertEqual(sorted(s1.union(s2)), [1, 2, 3, 4, 5])
        self.assertEqual(sorted(s1 | s2 | s3), [1, 2, 3, 4, 5, 6])
        self.assertEqual(sorted(s1.union(s2, s3)), [1, 2, 3, 4, 5, 6])
        self.assertRaises(TypeError, lambda: s1 | s2 | l)
        self.assertEqual(sorted(s1.union(s2, l)), [1, 2, 3, 4, 5, 6])

    def test_intersection(self):
        s1 = self.create_set([1, 2, 3, 3])
        s2 = self.create_set([3, 4, 5])
        s3 = set([6])
        l = [6]
        self.assertEqual(sorted(s1 & s2), [3])
        self.assertEqual(sorted(s1.intersection(s2)), [3])
        self.assertEqual(sorted(s1 & s2 & s3), [])
        self.assertEqual(sorted(s1.intersection(s2, s3)), [])
        self.assertRaises(TypeError, lambda: s1 & s2 & l)
        self.assertEqual(sorted(s1.intersection(s2, l)), [])
        self.assertEqual(sorted(s3 & s2), [])

    def test_difference(self):
        s1 = self.create_set([1, 2, 3, 3])
        s2 = self.create_set([3, 4, 5])
        s3 = set([6])
        l = [6]
        self.assertEqual(sorted(s1 - s2), [1, 2])
        self.assertEqual(sorted(s1.difference(s2)), [1, 2])
        self.assertEqual(sorted(s1 - s2 - s3), [1, 2])
        self.assertEqual(sorted(s1.difference(s2, s3)), [1, 2])
        self.assertRaises(TypeError, lambda: s1 - s2 - l)
        self.assertEqual(sorted(s1.difference(s2, l)), [1, 2])
        self.assertEqual(sorted(s3 - s1), [6])

    def test_symmetric_difference(self):
        s1 = self.create_set([1, 2, 3, 3])
        s2 = self.create_set([3, 4, 5])
        s3 = set([6])
        l = [6]
        self.assertEqual(sorted(s1 ^ s2), [1, 2, 4, 5])
        self.assertEqual(sorted(s1.symmetric_difference(s2)), [1, 2, 4, 5])
        self.assertEqual(sorted(s1 ^ s2 ^ s3), [1, 2, 4, 5, 6])
        self.assertRaises(TypeError, lambda: s1 ^ s2 ^ l)
        self.assertEqual(sorted(s3 ^ s1 ^ s2), [1, 2, 4, 5, 6])

    def test_copy(self):
        s1 = self.create_set('abc')
        s2 = s1.copy()
        self.assertEqual(s2.__class__, Set)
        self.assertEqual(sorted(s1),
                         sorted(s2))

    def test_result_type(self):
        s1 = self.create_set('ab')
        s2 = set('bc')
        s3 = s1 | s2
        s4 = s2 | s1
        self.assertEqual(s3.__class__, s1.__class__)
        self.assertEqual(s4.__class__, s2.__class__)

    def test_update(self):
        s1 = self.create_set('ab')
        s2 = frozenset('bc')
        st = 'cd'
        s1 |= s2
        self.assertEqual(sorted(s1), ['a', 'b', 'c'])
        s1.update(s2, st)
        self.assertEqual(sorted(s1), ['a', 'b', 'c', 'd'])

    def test_intersection_update(self):
        s1 = self.create_set('ab')
        s2 = frozenset('bc')
        st = 'cd'
        s1 &= s2
        self.assertEqual(sorted(s1), ['b'])
        s1.intersection_update(s2, st)
        self.assertEqual(sorted(s1), [])

    def test_difference_update(self):
        s1 = self.create_set('ab')
        s2 = frozenset('bc')
        st = 'cd'
        s1 -= s2
        self.assertEqual(sorted(s1), ['a'])
        s1.difference_update(s2, st)
        self.assertEqual(sorted(s1), ['a'])

    def test_symmetric_difference_update(self):
        s1 = self.create_set('ab')
        s2 = frozenset('bc')
        st = 'cd'
        s1 ^= s2
        self.assertEqual(sorted(s1), ['a', 'c'])
        s1.symmetric_difference_update(st)
        self.assertEqual(sorted(s1), ['a', 'd'])

    def test_add(self):
        s = self.create_set('ab')
        s.add('c')
        self.assertEqual(sorted(s), ['a', 'b', 'c'])
        self.assertFalse(s.add('c'))
        self.assertTrue(s.add('d'))

    def test_remove_discard(self):
        s = self.create_set('cdab')
        self.assertRaises(KeyError, s.remove, 'x')
        s.remove('b')
        self.assertEqual(sorted(s), ['a', 'c', 'd'])
        s.discard('x')
        s.discard('a')
        self.assertEqual(sorted(s), ['c', 'd'])

    def test_pop(self):
        s = self.create_set('a')
        self.assertEqual(s.pop(), 'a')
        self.assertEqual(sorted(s), [])
        self.assertRaises(KeyError, s.pop)

    def test_random_sample(self):
        s = self.create_set('a')
        self.assertEqual(s.random_sample(), ['a'])

        version = map(int, self.redis.info()['redis_version'].split('.'))
        major_ver, minor_ver, _ = version

        if major_ver >= 2 and minor_ver >= 6:
            s = self.create_set('ab')
            self.assertEqual(sorted(s.random_sample(2)), ['a', 'b'])

    def test_add_unicode(self):
        s = self.create_set()
        elem = u'ěščřžýáíéůú\U0001F4A9'
        s.add(elem)
        self.assertEqual(sorted(s), [elem])

    def test_clear(self):
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
