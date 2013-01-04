#!/usr/bin/env python
# -*- coding: utf-8 -*-


import redis
import unittest

from redis_collections import Dict


class DictTest(unittest.TestCase):
    # http://docs.python.org/2/library/stdtypes.html#mapping-types-dict

    db = 15

    def setUp(self):
        self.redis = redis.StrictRedis(db=self.db)
        if self.redis.dbsize():
            raise EnvironmentError('Redis database number %d is not empty, '
                                   'tests could harm your data.' % self.db)

    def test_set_get(self):
        d = Dict()
        d['a'] = 'b'
        self.assertEqual(d['a'], 'b')

    def test_init(self):
        d = Dict(one=1, two=2, three=3)
        self.assertEqual(sorted(d.items()),
                         [('one', 1), ('three', 3), ('two', 2)])
        d = Dict(zip(['one', 'two', 'three'], [1, 2, 3]))
        self.assertEqual(sorted(d.items()),
                         [('one', 1), ('three', 3), ('two', 2)])
        d = Dict([('two', 2), ('one', 1), ('three', 3)])
        self.assertEqual(sorted(d.items()),
                         [('one', 1), ('three', 3), ('two', 2)])
        d = Dict({'three': 3, 'one': 1, 'two': 2})
        self.assertEqual(sorted(d.items()),
                         [('one', 1), ('three', 3), ('two', 2)])
        d = Dict({'three': 3, 'one': 1, 'two': 2}, four=4)
        self.assertEqual(sorted(d.items()),
                         [('one', 1), ('four', 4), ('three', 3), ('two', 2)])
        d = Dict({'three': 3, 'one': 1, 'two': 2}, one=4)
        self.assertEqual(sorted(d.items()),
                         [('one', 4), ('three', 3), ('two', 2)])

    def test_len(self):
        d = Dict()
        self.assertEqual(len(d), 0)
        d['a'] = 'b'
        self.assertEqual(len(d), 1)
        d['c'] = 'd'
        self.assertEqual(len(d), 2)
        self.assertRaises(KeyError, lambda d: d['x'], d)

    def test_del(self):
        d = Dict()
        self.assertEqual(len(d), 0)
        d['a'] = 'b'
        self.assertEqual(len(d), 1)
        del d['a']
        self.assertEqual(len(d), 0)
        self.assertRaises(KeyError, lambda d: d['a'], d)

    def test_in(self):
        d = Dict()
        d['a'] = 'b'
        self.assertTrue('a' in d)
        self.assertFalse('c' in d)
        self.assertFalse('a' not in d)
        self.assertTrue('c' not in d)

    def test_items(self):
        d = Dict()
        d['a'] = 'b'
        d['c'] = 'd'
        self.assertEqual(sorted(d.items()),
                         [('a', 'b'), ('c', 'd')])
        self.assertEqual(sorted(d.iteritems()),
                         [('a', 'b'), ('c', 'd')])
        self.assertTrue(hasattr(d.iteritems(), 'next'))

    def test_copy(self):
        d1 = Dict()
        d1['a'] = 'b'
        d1['c'] = 'd'
        d2 = d1.copy()
        self.assertEqual(sorted(d1.items()),
                         sorted(d2.items()))

    def test_get(self):
        d = Dict()
        d['a'] = 'b'
        self.assertEqual(d.get('a'), 'b')
        self.assertEqual(d.get('c'), None)
        self.assertEqual(d.get('a', 'x'), 'b')
        self.assertEqual(d.get('c', 'x'), 'x')

    def test_keys(self):
        d = Dict()
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
        d = Dict()
        d['a'] = 'b'
        d['c'] = 'd'
        self.assertEqual(sorted(d.values()),
                         ['b', 'd'])
        self.assertEqual(sorted(d.itervalues()),
                         ['b', 'd'])
        self.assertTrue(hasattr(d.itervalues(), 'next'))

    def test_fromkeys(self):
        d = Dict.fromkeys(['a', 'b', 'c', 'd'])
        self.assertEqual(sorted(d.keys()),
                         ['a', 'b', 'c', 'd'])
        self.assertEqual(d.values(),
                         [None] * 4)

        d = Dict.fromkeys(['a', 'b', 'c', 'd'], 'be happy')
        self.assertEqual(sorted(d.keys()),
                         ['a', 'b', 'c', 'd'])
        self.assertEqual(d.values(),
                         ['be happy'] * 4)

    def test_clear(self):
        d = Dict()
        d['a'] = 'b'
        d['c'] = 'd'
        d.clear()
        self.assertEqual(d.items(), [])

    def test_pop(self):
        d = Dict()
        d['a'] = 'b'
        self.assertEqual(d.pop('a'), 'b')
        self.assertEqual(d.pop('a', 'x'), 'x')
        self.assertRaises(KeyError, d.pop, 'a')

    def test_popitem(self):
        d = Dict()
        d['a'] = 'b'
        self.assertEqual(d.popitem(), ('a', 'b'))
        self.assertRaises(KeyError, d.popitem)

    def test_setdefault(self):
        d = Dict()
        d['a'] = 'b'
        self.assertEqual(d.setdefault('a'), 'b')
        self.assertEqual(d.setdefault('c'), None)
        self.assertEqual(d.setdefault('x', 42), 42)

    def test_update(self):
        d = Dict()
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

    def tearDown(self):
        self.redis.flushdb()


if __name__ == '__main__':
    unittest.main()
