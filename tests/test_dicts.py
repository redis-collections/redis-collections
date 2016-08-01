#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, unicode_literals

import collections
import operator
import unittest

import six

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
        self.assertEqual(sorted(d1.items()), sorted(d2.items()))

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
        for init in (self.create_counter, collections.Counter):
            c = init()
            c['a'] = 5
            self.assertEqual(c['a'], 5)

    def test_init(self):
        for init in (self.create_counter, collections.Counter):
            c = init('gallahad')
            self.assertEqual(c['a'], 3)
            self.assertEqual(c['l'], 2)
            self.assertEqual(c['g'], 1)

            c = init([1, 1, 2, 2, 2, 38])
            self.assertEqual(c[1], 2)
            self.assertEqual(c[2], 3)
            self.assertEqual(c[38], 1)

            c = init({'red': 4, 'blue': 2})
            self.assertEqual(c['red'], 4)
            self.assertEqual(c['blue'], 2)

    def test_missing(self):
        for init in (self.create_counter, collections.Counter):
            c = init('gallahad')
            self.assertEqual(c['x'], 0)

    def test_del(self):
        for init in (self.create_counter, collections.Counter):
            c = init('gallahad')
            self.assertEqual(c['x'], 0)

            self.assertFalse('x' in c)
            self.assertEqual(c['x'], 0)

            c['x'] = 0
            self.assertTrue('x' in c)
            self.assertEqual(c['x'], 0)

            del c['x']
            self.assertFalse('x' in c)
            self.assertEqual(c['x'], 0)

            try:
                del c['x']
            except KeyError:
                self.fail('Counter.__delitem__ should not raise KeyError')

    def test_elements(self):
        for init in (self.create_counter, collections.Counter):
            c = init({'a': 3, 'b': 0, 'c': 1, 'd': -5})
            self.assertEqual(sorted(c.elements()), ['a', 'a', 'a', 'c'])

    def test_most_common(self):
        for init in (self.create_counter, collections.Counter):
            c = init('abbcccddddeeeeeffffff')
            counts = [
                ('f', 6), ('e', 5), ('d', 4), ('c', 3), ('b', 2), ('a', 1)
            ]
            self.assertEqual(c.most_common(), counts)
            self.assertEqual(c.most_common(1), counts[:1])
            self.assertEqual(c.most_common(3), counts[:3])

    def test_subtract(self):
        expected_result = [('a', -1), ('b', 0), ('c', 1)]
        for init in (self.create_counter, collections.Counter):
            # Both Counters
            c_1 = init('abbccc')
            c_2 = init('aabbcc')
            c_1.subtract(c_2)
            self.assertEqual(sorted(c_1.items()), expected_result)

            # One Counter, one dict
            c = init('abbccc')
            c.subtract({'a': 2, 'b': 2, 'c': 2})
            self.assertEqual(sorted(c.items()), expected_result)

            # One Counter, one sequence and kwargs
            c = init('abbccc')
            c.subtract(['a', 'a', 'b', 'b'], c=2)
            self.assertEqual(sorted(c.items()), expected_result)

    def test_fromkeys(self):
        self.assertRaises(NotImplementedError, Counter.fromkeys, [1, 2])

    def test_update(self):
        expected_result = [('a', 3), ('b', 4), ('c', 5)]
        for init in (self.create_counter, collections.Counter):
            # Both Counters
            c_1 = init('abbccc')
            c_2 = init('aabbcc')
            c_1.update(c_2)
            self.assertEqual(sorted(c_1.items()), expected_result)

            # One Counter, one dict
            c = init('abbccc')
            c.update({'a': 2, 'b': 2, 'c': 2})
            self.assertEqual(sorted(c.items()), expected_result)

            # One Counter, one sequence and kwargs
            c = init('abbccc')
            c.update(['a', 'a', 'b', 'b'], c=2)
            self.assertEqual(sorted(c.items()), expected_result)

    def _test_op(self, op, do_reverse=True):
        redis_counter = self.create_counter('abbccc')
        python_counter = collections.Counter('abbccc')

        redis_other = self.create_counter('aabbcc')
        python_other = collections.Counter('aabbcc')

        # Same types
        self.assertEqual(
            op(redis_counter, redis_other), op(python_counter, python_other)
        )
        self.assertEqual(
            op(redis_other, redis_counter), op(python_other, python_counter)
        )

        # Different types
        self.assertEqual(
            op(redis_counter, python_other), op(python_counter, python_other)
        )

        # Reversed argument order
        if do_reverse:
            self.assertEqual(
                op(python_other, redis_counter),
                op(python_other, python_counter)
            )

        # Fail for non-counter types
        for c in (redis_counter, python_counter):
            with self.assertRaises(TypeError):
                op(c, {'a': 2, 'b': 2, 'c': 2})

    def test_add(self):
        self._test_op(operator.add)

        result = self.create_counter('abbccc') + self.create_counter('aabbcc')
        self.assertTrue(isinstance(result, Counter))
        self.assertEqual(result, {'a': 3, 'b': 4, 'c': 5})

    def test_sub(self):
        self._test_op(operator.sub)

        result = self.create_counter('abbccc') - self.create_counter('aabbcc')
        self.assertTrue(isinstance(result, Counter))
        self.assertEqual(result, {'c': 1})

    def test_or(self):
        self._test_op(operator.or_, do_reverse=False)

        result = self.create_counter('abbccc') | self.create_counter('aabbcc')
        self.assertTrue(isinstance(result, Counter))
        self.assertEqual(result, {'a': 2, 'b': 2, 'c': 3})

    def test_and(self):
        self._test_op(operator.and_, do_reverse=False)

        result = self.create_counter('abbccc') & self.create_counter('aabbcc')
        self.assertTrue(isinstance(result, Counter))
        self.assertEqual(result, {'a': 1, 'b': 2, 'c': 2})

    def test_iadd(self):
        redis_counter = self.create_counter('ab')
        python_counter = collections.Counter('ab')

        # Same types
        redis_counter += self.create_counter('bcc')
        python_counter += collections.Counter('bcc')
        self.assertEqual(redis_counter, python_counter)

        # Different types
        redis_counter += collections.Counter('cdddd')
        python_counter += collections.Counter('cdddd')
        self.assertEqual(redis_counter, python_counter)

    def test_isub(self):
        redis_counter = self.create_counter('ab')
        python_counter = collections.Counter('ab')

        # Same types
        redis_counter -= self.create_counter('bcc')
        python_counter -= collections.Counter('bcc')
        self.assertEqual(redis_counter, python_counter)

        # Different types
        redis_counter -= collections.Counter('cdddd')
        python_counter -= collections.Counter('cdddd')
        self.assertEqual(redis_counter, python_counter)

    def test_ior(self):
        redis_counter = self.create_counter('ab')
        python_counter = collections.Counter('ab')

        # Same types
        redis_counter |= self.create_counter('bcc')
        python_counter |= collections.Counter('bcc')
        self.assertEqual(redis_counter, python_counter)

        # Different types
        redis_counter |= collections.Counter('cdddd')
        python_counter |= collections.Counter('cdddd')
        self.assertEqual(redis_counter, python_counter)

    def test_iand(self):
        redis_counter = self.create_counter('ab')
        python_counter = collections.Counter('ab')

        # Same types
        redis_counter &= self.create_counter('bcc')
        python_counter &= collections.Counter('bcc')
        self.assertEqual(redis_counter, python_counter)

        # Different types
        redis_counter &= collections.Counter('cdddd')
        python_counter &= collections.Counter('cdddd')
        self.assertEqual(redis_counter, python_counter)

    if not six.PY2:
        def test_pos(self):
            redis_counter = self.create_counter({'a': 1, 'b': -2, 'c': 3})
            python_counter = collections.Counter({'a': 1, 'b': -2, 'c': 3})

            self.assertEqual(+redis_counter, +python_counter)

        def test_neg(self):
            redis_counter = self.create_counter({'a': 1, 'b': -2, 'c': 3})
            python_counter = collections.Counter({'a': 1, 'b': -2, 'c': 3})

            self.assertEqual(-redis_counter, -python_counter)

if __name__ == '__main__':
    unittest.main()
