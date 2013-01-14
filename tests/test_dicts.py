#!/usr/bin/env python
# -*- coding: utf-8 -*-


import unittest

from .base import RedisTestCase
from redis_collections import Dict


class DictTest(RedisTestCase):

    def create_dict(self, *args, **kwargs):
        kwargs['redis'] = self.redis
        return Dict(*args, **kwargs)

    def test_set_get(self):
        d = self.create_dict()
        d['a'] = 'b'
        self.assertEqual(d['a'], 'b')

    def test_get_many(self):
        d = self.create_dict()
        d['a'] = 'b'
        d['c'] = 'd'
        d['e'] = 'f'
        self.assertEqual(d.get_many('a', 'e', 'x'), ['b', 'f', None])

    def test_init(self):
        d = self.create_dict(zip(['one', 'two', 'three'], [1, 2, 3]))
        self.assertEqual(sorted(d.items()),
                         [('one', 1), ('three', 3), ('two', 2)])
        d = self.create_dict([('two', 2), ('one', 1), ('three', 3)])
        self.assertEqual(sorted(d.items()),
                         [('one', 1), ('three', 3), ('two', 2)])
        d = self.create_dict({'three': 3, 'one': 1, 'two': 2})
        self.assertEqual(sorted(d.items()),
                         [('one', 1), ('three', 3), ('two', 2)])

    def test_id(self):
        d1 = self.create_dict()
        d1['a'] = 'b'
        d2 = self.create_dict(id=d1.id)
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
        self.assertEqual(sorted(d.items()),
                         [('a', 'b'), ('c', 'd')])
        self.assertEqual(sorted(d.iteritems()),
                         [('a', 'b'), ('c', 'd')])
        self.assertTrue(hasattr(d.iteritems(), 'next'))

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
        self.assertEqual(sorted(d.keys()),
                         ['a', 'c'])
        self.assertEqual(sorted(d.iterkeys()),
                         ['a', 'c'])
        self.assertTrue(hasattr(d.iterkeys(), 'next'))
        self.assertEqual(sorted(d.iter()),
                         ['a', 'c'])
        self.assertTrue(hasattr(d.iter(), 'next'))

    def test_values(self):
        d = self.create_dict()
        d['a'] = 'b'
        d['c'] = 'd'
        self.assertEqual(sorted(d.values()),
                         ['b', 'd'])
        self.assertEqual(sorted(d.itervalues()),
                         ['b', 'd'])
        self.assertTrue(hasattr(d.itervalues(), 'next'))

    def test_fromkeys(self):
        d = Dict.fromkeys(['a', 'b', 'c', 'd'], redis=self.redis)
        self.assertEqual(sorted(d.keys()),
                         ['a', 'b', 'c', 'd'])
        self.assertEqual(d.values(),
                         [None] * 4)

        d = Dict.fromkeys(['a', 'b', 'c', 'd'], 'be happy', redis=self.redis)
        self.assertEqual(sorted(d.keys()),
                         ['a', 'b', 'c', 'd'])
        self.assertEqual(d.values(),
                         ['be happy'] * 4)

    def test_clear(self):
        d = self.create_dict()
        d['a'] = 'b'
        d['c'] = 'd'
        d.clear()
        self.assertEqual(d.items(), [])

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
        self.assertEqual(sorted(d.items()),
                         [('a', 'b'), ('c', 'd')])
        d.update({'c': 42})
        self.assertEqual(sorted(d.items()),
                         [('a', 'b'), ('c', 42)])
        d.update({'x': 38})
        self.assertEqual(sorted(d.items()),
                         [('a', 'b'), ('c', 42), ('x', 38)])
        d.update([('a', 'g')])
        self.assertEqual(sorted(d.items()),
                         [('a', 'g'), ('c', 42), ('x', 38)])
        d.update(c=None)
        self.assertEqual(sorted(d.items()),
                         [('a', 'g'), ('c', None), ('x', 38)])

if __name__ == '__main__':
    unittest.main()
