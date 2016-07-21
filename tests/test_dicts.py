#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, unicode_literals

import unittest

from redis_collections import Dict, Counter

from .base import RedisTestCase


class DictTest(RedisTestCase):

    def create_dict(self, *args, **kwargs):
        kwargs['redis'] = self.redis
        return Dict(*args, **kwargs)

    def test_set_get(self):
        d = self.create_dict()
        d['a'] = 'b'
        self.assertEqual(d['a'], 'b')

    def test_getmany(self):
        d = self.create_dict()
        d['a'] = 'b'
        d['c'] = 'd'
        d['e'] = 'f'
        self.assertEqual(d.getmany('a', 'e', 'x'), ['b', 'f', None])

    def test_init(self):
        d = self.create_dict(zip(['one', 'two', 'three'], [1, 2, 3]))
        self.assertEqual(
            sorted(d.items()), [('one', 1), ('three', 3), ('two', 2)]
        )

        d = self.create_dict([('two', 2), ('one', 1), ('three', 3)])
        self.assertEqual(
            sorted(d.items()), [('one', 1), ('three', 3), ('two', 2)]
        )

        d = self.create_dict({'three': 3, 'one': 1, 'two': 2})
        self.assertEqual(
            sorted(d.items()), [('one', 1), ('three', 3), ('two', 2)]
        )

    def test_key(self):
        d1 = self.create_dict()
        d1['a'] = 'b'
        d2 = self.create_dict(key=d1.key)
        self.assertEqual(d1, d2)
        self.assertEqual(sorted(d1.items()), sorted(d2.items()))

    def test_len(self):
        d = self.create_dict()
        self.assertEqual(len(d), 0)
        d['a'] = 'b'
        self.assertEqual(len(d), 1)
        d['c'] = 'd'
        self.assertEqual(len(d), 2)
        self.assertRaises(KeyError, lambda: d['x'])

    def test_del(self):
        d = self.create_dict()
        self.assertEqual(len(d), 0)
        d['a'] = 'b'
        self.assertEqual(len(d), 1)
        del d['a']
        self.assertEqual(len(d), 0)
        self.assertRaises(KeyError, lambda: d['a'])

    def test_in(self):
        d = self.create_dict()
        d['a'] = 'b'
        self.assertTrue('a' in d)
        self.assertFalse('c' in d)
        self.assertFalse('a' not in d)
        self.assertTrue('c' not in d)

    def test_items(self):
        d = self.create_dict()
        d['a'] = 'b'
        d['c'] = 'd'
        self.assertEqual(sorted(d.items()), [('a', 'b'), ('c', 'd')])
        self.assertEqual(sorted(d.iteritems()), [('a', 'b'), ('c', 'd')])
        try:
            next(d.iteritems())
        except AttributeError:
            self.fail()

    def test_copy(self):
        d1 = self.create_dict()
        d1['a'] = 'b'
        d1['c'] = 'd'
        d2 = d1.copy()
        self.assertEqual(d2.__class__, Dict)
        self.assertEqual(sorted(d1.items()),
                         sorted(d2.items()))

    def test_get(self):
        d = self.create_dict()
        d['a'] = 'b'
        self.assertEqual(d.get('a'), 'b')
        self.assertEqual(d.get('c'), None)
        self.assertEqual(d.get('a', 'x'), 'b')
        self.assertEqual(d.get('c', 'x'), 'x')

    def test_keys(self):
        d = self.create_dict()
        d['a'] = 'b'
        d['c'] = 'd'
        self.assertEqual(sorted(d.keys()), ['a', 'c'])
        self.assertEqual(sorted(d.iterkeys()), ['a', 'c'])
        self.assertEqual(sorted(d.iter()), ['a', 'c'])
        try:
            next(d.iter())
        except AttributeError:
            self.fail()

    def test_values(self):
        d = self.create_dict()
        d['a'] = 'b'
        d['c'] = 'd'
        self.assertEqual(sorted(d.values()), ['b', 'd'])
        self.assertEqual(sorted(d.itervalues()), ['b', 'd'])
        try:
            next(d.itervalues())
        except AttributeError:
            self.fail()

    def test_fromkeys(self):
        d = Dict.fromkeys(['a', 'b', 'c', 'd'], redis=self.redis)
        self.assertEqual(sorted(d.keys()), ['a', 'b', 'c', 'd'])
        self.assertEqual(d.values(), [None] * 4)

        d = Dict.fromkeys(['a', 'b', 'c', 'd'], 'be happy', redis=self.redis)
        self.assertEqual(sorted(d.keys()), ['a', 'b', 'c', 'd'])
        self.assertEqual(d.values(), ['be happy'] * 4)

    def test_clear(self):
        d = self.create_dict()
        d['a'] = 'b'
        d['c'] = 'd'
        d.clear()
        self.assertEqual(d.items(), [])
        self.assertEqual(self.redis.dbsize(), 0)

    def test_pop(self):
        d = self.create_dict()
        d['a'] = 'b'
        self.assertEqual(d.pop('a'), 'b')
        self.assertEqual(d.pop('a', 'x'), 'x')
        self.assertRaises(KeyError, d.pop, 'a')

    def test_popitem(self):
        d = self.create_dict()
        d['a'] = 'b'
        self.assertEqual(d.popitem(), ('a', 'b'))
        self.assertRaises(KeyError, d.popitem)

    def test_setdefault(self):
        d = self.create_dict()
        d['a'] = 'b'
        self.assertEqual(d.setdefault('a'), 'b')
        self.assertEqual(d.setdefault('c'), None)
        self.assertEqual(d.setdefault('x', 42), 42)

    def test_update(self):
        d = self.create_dict()
        d['a'] = 'b'

        d.update({'c': 'd'})
        self.assertEqual(sorted(d.items()), [('a', 'b'), ('c', 'd')])

        d.update({'c': 42})
        self.assertEqual(sorted(d.items()), [('a', 'b'), ('c', 42)])

        d.update({'x': 38})
        self.assertEqual(
            sorted(d.items()), [('a', 'b'), ('c', 42), ('x', 38)]
        )

        d.update([('a', 'g')])
        self.assertEqual(
            sorted(d.items()), [('a', 'g'), ('c', 42), ('x', 38)]
        )
        d.update(c=None)
        self.assertEqual(
            sorted(d.items()), [('a', 'g'), ('c', None), ('x', 38)]
        )

    def test_get_default(self):
        d = self.create_dict()
        for ff in ('', False, None, 0):
            d['h'] = ff
            self.assertEqual(d.get('h', 'wrong'), ff)


class CounterTest(RedisTestCase):

    def create_counter(self, *args, **kwargs):
        kwargs['redis'] = self.redis
        return Counter(*args, **kwargs)

    def test_set_get(self):
        c = self.create_counter()
        c['a'] = 5
        self.assertEqual(c['a'], 5)

    def test_init(self):
        c = self.create_counter('gallahad')
        self.assertEqual(c['a'], 3)
        self.assertEqual(c['l'], 2)
        self.assertEqual(c['g'], 1)

        c = self.create_counter([1, 1, 2, 2, 2, 38])
        self.assertEqual(c[1], 2)
        self.assertEqual(c[2], 3)
        self.assertEqual(c[38], 1)

        c = self.create_counter({'red': 4, 'blue': 2})
        self.assertEqual(c['red'], 4)
        self.assertEqual(c['blue'], 2)

    def test_missing(self):
        c = self.create_counter('gallahad')
        self.assertEqual(c['x'], 0)

    def test_del(self):
        c = self.create_counter('gallahad')
        self.assertFalse('x' in c)
        self.assertEqual(c['x'], 0)
        c['x'] = 0
        self.assertTrue('x' in c)
        self.assertEqual(c['x'], 0)
        del c['x']
        self.assertFalse('x' in c)
        self.assertEqual(c['x'], 0)

    def test_elements(self):
        c = self.create_counter({'a': 3, 'b': 0, 'c': 1, 'd': -5})
        self.assertEqual(sorted(c.elements()), ['a', 'a', 'a', 'c'])

    def test_most_common(self):
        c = self.create_counter('abbcccddddeeeeeffffff')
        counts = [
            ('f', 6), ('e', 5), ('d', 4), ('c', 3), ('b', 2), ('a', 1)
        ]
        self.assertEqual(c.most_common(), counts)
        self.assertEqual(c.most_common(1), counts[0:1])
        self.assertEqual(c.most_common(3), counts[:3])

    def test_subtract(self):
        result = [
            ('a', 0), ('b', 1), ('c', 1), ('d', 2), ('e', 2), ('f', 3)
        ]

        c1 = self.create_counter('abbcccddddeeeeeffffff')
        c1.subtract('abccddeeefff')
        self.assertEqual(sorted(c1.items()), sorted(result))

        c1 = self.create_counter('abbcccddddeeeeeffffff')
        c2 = self.create_counter('abccddeeefff')
        c1.subtract(c2)
        self.assertEqual(sorted(c1.items()), sorted(result))

    def test_fromkeys(self):
        self.assertRaises(NotImplementedError, Counter.fromkeys, [1, 2])

    def test_update(self):
        result = [
            ('a', 2), ('b', 3), ('c', 5), ('d', 6), ('e', 8), ('f', 9)
        ]

        c1 = self.create_counter('abbcccddddeeeeeffffff')
        c1.update('abccddeeefff')
        self.assertEqual(sorted(c1.items()), sorted(result))

        c1 = self.create_counter('abbcccddddeeeeeffffff')
        c2 = self.create_counter('abccddeeefff')
        c1.update(c2)
        self.assertEqual(sorted(c1.items()), sorted(result))

    def test_add(self):
        c1 = self.create_counter('abbcccddddeeeeeffffff')
        c2 = self.create_counter('abccddeeefff')
        result = [
            ('a', 2), ('b', 3), ('c', 5), ('d', 6), ('e', 8), ('f', 9)
        ]
        self.assertEqual(sorted((c1 + c2).items()), sorted(result))

    def test_diff(self):
        c1 = self.create_counter('abbcccddddeeeeeffffff')
        c2 = self.create_counter('abccddeeefff')
        result = [('b', 1), ('c', 1), ('d', 2), ('e', 2), ('f', 3)]
        self.assertEqual(sorted((c1 - c2).items()), sorted(result))

    def test_and(self):
        c1 = self.create_counter('abbcccddddeeeeef')
        c2 = self.create_counter('abccddeeefff')
        result = [
            ('a', 1), ('b', 1), ('c', 2), ('d', 2), ('e', 3), ('f', 1)
        ]
        self.assertEqual(sorted((c1 & c2).items()), sorted(result))

    def test_or(self):
        c1 = self.create_counter('abbcccddddeeeeef')
        c2 = self.create_counter('abccddeeefff')
        result = [
            ('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', 5), ('f', 3)
        ]
        self.assertEqual(sorted((c1 | c2).items()), sorted(result))

    def test_inc(self):
        c = self.create_counter('abbcccc')
        c.inc('x', 0)
        self.assertFalse('x' in c)
        c.inc('b')
        self.assertEqual(c['b'], 3)
        c.inc('b', 5)
        self.assertEqual(c['b'], 8)
        c.inc('b', -1)
        self.assertEqual(c['b'], 7)
        c.inc('b', -9)
        self.assertEqual(c['b'], -2)


if __name__ == '__main__':
    unittest.main()
