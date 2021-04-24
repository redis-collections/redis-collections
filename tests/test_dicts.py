import collections
import operator
import sys
import unittest

from redis_collections import Counter, DefaultDict, Dict, List

from .base import RedisTestCase


class DictTest(RedisTestCase):
    def create_dict(self, *args, **kwargs):
        kwargs['redis'] = self.redis
        return Dict(*args, **kwargs)

    def test_set_get(self):
        redis_dict = self.create_dict()
        python_dict = {}

        for k, v in [
            ('a', 1),
            (b'a', 2),
            ('a', 3),
            (1, 'one'),
            (1.0, 'one point zero'),
        ]:
            redis_dict[k] = v
            python_dict[k] = v
            self.assertEqual(redis_dict[k], python_dict[k])

    def test_getmany(self):
        d = self.create_dict()
        d['a'] = 'b'
        d['c'] = 'd'
        d['e'] = 'f'
        d[1] = 'g'
        self.assertEqual(d.getmany('a', 'e', 1.0, 'x'), ['b', 'f', 'g', None])
        self.assertEqual(d.getmany(b'a', b'c'), [None, None])

    def test_init(self):
        init_seq = [
            ('a', 1),
            (b'a', 2),
            ('a', 3),
            (1, 'one'),
            (1.0, 'one point zero'),
        ]
        redis_dict = self.create_dict(init_seq)
        python_dict = dict(init_seq)
        self.assertCountEqual(redis_dict.items(), python_dict.items())

        init_dict = dict(
            [
                ('a', 1),
                (b'a', 2),
                ('a', 3),
                (1, 'one'),
                (1.0, 'one point zero'),
            ]
        )
        redis_dict = self.create_dict(init_dict)
        python_dict = dict(init_dict)
        self.assertCountEqual(redis_dict.items(), python_dict.items())

    def test_key(self):
        d1 = self.create_dict()
        d1['a'] = 'b'
        d2 = self.create_dict(key=d1.key)
        self.assertEqual(d1, d2)
        self.assertEqual(sorted(d1.items()), sorted(d2.items()))

    def test_len(self):
        redis_dict = self.create_dict()
        python_dict = {}

        for k, v in [
            ('a', 1),
            (b'a', 2),
            ('a', 3),
            (1, 'one'),
            (1.0, 'one point zero'),
        ]:
            redis_dict[k] = v
            python_dict[k] = v
            self.assertEqual(len(redis_dict), len(python_dict))

    def test_del(self):
        redis_dict = self.create_dict([('a', 1), (b'a', 2), (2, 'b')])
        python_dict = dict([('a', 1), (b'a', 2), (2, 'b')])

        for key in ('a', 2):
            del redis_dict[key]
            del python_dict[key]
            self.assertEqual(redis_dict, python_dict)

        for D in (redis_dict, python_dict):
            with self.assertRaises(KeyError):
                del D[2]

        # b'a' and 'a' hash to the same thing but aren't equal in Python 3
        del redis_dict[b'a']
        del python_dict[b'a']
        self.assertEqual(redis_dict, python_dict)

    def test_in(self):
        redis_dict = self.create_dict()
        python_dict = {}
        for D in (redis_dict, python_dict):
            D['a'] = 'b'
            D[1.0] = 'one point zero'

            self.assertIn('a', D)
            self.assertIn(1.0, D)
            self.assertIn(1, D)
            self.assertNotIn('b', D)
            self.assertNotIn(b'a', D)

    def test_items(self):
        d = self.create_dict()
        d['a'] = 'b'
        d['c'] = 'd'
        self.assertEqual(sorted(d.items()), [('a', 'b'), ('c', 'd')])
        try:
            next(d.items())
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
        try:
            next(d.keys())
        except AttributeError:
            self.fail()

    def test_values(self):
        d = self.create_dict()
        d['a'] = 'b'
        d['c'] = 'd'
        self.assertEqual(sorted(d.values()), ['b', 'd'])
        try:
            next(d.values())
        except AttributeError:
            self.fail()

    def test_fromkeys(self):
        d = Dict.fromkeys(['a', 'b', 'c', 'd'], redis=self.redis)
        self.assertEqual(sorted(d.keys()), ['a', 'b', 'c', 'd'])
        self.assertEqual(list(d.values()), [None] * 4)

        d = Dict.fromkeys(['a', 'b', 'c', 'd'], 'be happy', redis=self.redis)
        self.assertEqual(sorted(d.keys()), ['a', 'b', 'c', 'd'])
        self.assertEqual(list(d.values()), ['be happy'] * 4)

    def test_clear(self):
        d = self.create_dict()
        d['a'] = 'b'
        d['c'] = 'd'
        d.clear()
        self.assertEqual(list(d.items()), [])
        self.assertEqual(self.redis.dbsize(), 0)

    def test_pop(self):
        redis_dict = self.create_dict()
        python_dict = {}
        for D in (redis_dict, python_dict):
            D['a'] = 1
            self.assertEqual(D.pop('a'), 1)
            self.assertNotIn('a', D)
            self.assertEqual(D.pop('a', b'default'), b'default')
            self.assertRaises(KeyError, D.pop, 'a')

            D['a'] = 1
            D[b'a'] = 2
            self.assertEqual(D.pop('a'), 1)
            self.assertNotIn('a', D)
            self.assertIn(b'a', D)

    def test_popitem(self):
        redis_dict = self.create_dict()
        python_dict = {}
        for D in (redis_dict, python_dict):
            D['a'] = 1
            self.assertEqual(D.popitem(), ('a', 1))
            self.assertNotIn('a', D)
            self.assertRaises(KeyError, D.popitem)

    def test_setdefault(self):
        d = self.create_dict()
        d['a'] = 'b'
        self.assertEqual(d.setdefault('a'), 'b')
        self.assertEqual(d.setdefault('c'), None)
        self.assertEqual(d.setdefault('x', 42), 42)

    def test_update(self):
        d = self.create_dict()
        d['a'] = 'b'

        # Update from built-in dicts
        d.update({'c': 'd'})
        self.assertEqual(sorted(d.items()), [('a', 'b'), ('c', 'd')])

        d.update({'c': 42})
        self.assertEqual(sorted(d.items()), [('a', 'b'), ('c', 42)])

        d.update({'x': 38})
        self.assertEqual(sorted(d.items()), [('a', 'b'), ('c', 42), ('x', 38)])

        # Update from list of tuples
        d.update([('a', 'g')])
        self.assertEqual(sorted(d.items()), [('a', 'g'), ('c', 42), ('x', 38)])

        # Update from kwargs
        d.update(c=None)
        self.assertEqual(
            sorted(d.items()), [('a', 'g'), ('c', None), ('x', 38)]
        )

        # Update from another redis_collections class
        redis_list = List([('a', 'h')], redis=self.redis)
        d.update(redis_list)
        self.assertEqual(
            sorted(d.items()), [('a', 'h'), ('c', None), ('x', 38)]
        )

    def test_get_default(self):
        d = self.create_dict()
        for ff in ('', False, None, 0):
            d['h'] = ff
            self.assertEqual(d.get('h', 'wrong'), ff)

    def test_mutable(self):
        redis_plain = self.create_dict()
        redis_cached = self.create_dict(writeback=True)
        python_dict = {}

        # Create a mutable entry and then modify it
        for D in (redis_plain, redis_cached, python_dict):
            D['list'] = [1]
            D['list'].append(2)

            D['set'] = {1}
            D['set'].add(2)

            D['dict'] = {1: 'one'}
            D['dict'][2] = 'two'

        # The Redis Dict with writeback=False won't have made the updates
        self.assertNotEqual(redis_plain['list'], python_dict['list'])
        self.assertNotEqual(redis_plain['set'], python_dict['set'])
        self.assertNotEqual(redis_plain['dict'], python_dict['dict'])

        # The Redis dict with writeback=True will have made the updates
        self.assertEqual(redis_cached['list'], python_dict['list'])
        self.assertEqual(redis_cached['set'], python_dict['set'])
        self.assertEqual(redis_cached['dict'], python_dict['dict'])

    def test_cache(self):
        redis_cached = self.create_dict(writeback=True)

        # Setting a key should add it to both the cache and to Redis
        redis_cached['key_1'] = [1]
        self.assertIn('key_1', redis_cached.cache)
        self.assertIn('key_1', redis_cached._data())

        # The mutated value should be reflected in items, values
        redis_cached['key_1'].append(2)
        self.assertEqual(list(redis_cached.items()), [('key_1', [1, 2])])
        self.assertEqual(list(redis_cached.values()), [([1, 2])])

        # sync-ing should push changes to Redis and clear the cache
        self.assertEqual(redis_cached._data()['key_1'], [1])
        redis_cached.sync()
        self.assertEqual(redis_cached._data()['key_1'], [1, 2])
        self.assertEqual(redis_cached.cache, {})

        # Deleting a key should delete it from both the cache and Redis
        redis_cached['key_2'] = [2]
        del redis_cached['key_2']
        self.assertNotIn('key_2', redis_cached.cache)
        self.assertNotIn('key_2', redis_cached._data())

        # Popping a key should delete it from both the cache and Redis
        redis_cached['key_3'] = [3]
        redis_cached['key_3'].append(4)
        self.assertEqual(redis_cached.pop('key_3'), [3, 4])
        self.assertNotIn('key_3', redis_cached.cache)
        self.assertNotIn('key_3', redis_cached._data())

        # Doing popitem should delete the item from both the cache and Redis
        redis_cached.clear()
        redis_cached['key_4'] = [4]
        redis_cached['key_4'].append(5)
        self.assertEqual(redis_cached.popitem(), ('key_4', [4, 5]))
        self.assertNotIn('key_4', redis_cached.cache)
        self.assertNotIn('key_4', redis_cached._data())

        # setdefault should reflect changes to mutable objects
        redis_cached['key_5'] = [5]
        redis_cached['key_5'].append(6)
        self.assertEqual(redis_cached.setdefault('key_5'), [5, 6])
        self.assertIn('key_5', redis_cached.cache)
        self.assertIn('key_5', redis_cached._data())

        self.assertEqual(redis_cached.setdefault('key_6', [6, 7]), [6, 7])
        self.assertIn('key_6', redis_cached.cache)
        self.assertIn('key_6', redis_cached._data())

        # update should update both the cache and Redis
        redis_cached.clear()
        redis_cached['key_7'] = [7]
        redis_cached.update({'key_7': [7, 8, 9], 'key_8': [9]})
        self.assertEqual(redis_cached._data()['key_7'], [7, 8, 9])
        self.assertEqual(redis_cached.cache['key_7'], [7, 8, 9])

    def test_with(self):
        with self.create_dict() as D:
            # Writeback set
            self.assertTrue(D.writeback)

            # Store a mutable value, modify it, and retrieve it - changes
            # should be reflected
            D['key'] = [1]
            D['key'].append(2)
            self.assertEqual(D['key'], [1, 2])

            # Changes are not in Redis yet
            self.assertEqual(D._data()['key'], [1])

        # Closing the context manager syncs to Redis
        self.assertEqual(D._data()['key'], [1, 2])

    def test_repr(self):
        redis_dict = self.create_dict(writeback=True)
        redis_dict[0] = {}
        redis_dict[0][1] = 2

        self.assertIn("{0: {1: 2}}", repr(redis_dict))

    def test_eq(self):
        data = {'a': 1, 'b': 2}
        redis_dict = self.create_dict(data)
        redis_cached = self.create_dict(data)
        python_dict = data.copy()

        self.assertEqual(redis_dict, python_dict)
        self.assertEqual(python_dict, redis_dict)

        self.assertEqual(redis_cached, python_dict)
        self.assertEqual(python_dict, redis_cached)

        self.assertNotEqual(redis_dict, data.items())
        self.assertNotEqual(redis_cached, data.items())
        self.assertNotEqual(python_dict, data.items())

    def test_marker(self):
        redis_dict = self.create_dict()
        self.assertEqual(repr(redis_dict._Dict__marker), '<missing value>')

    def test_scan_items(self):
        redis_dict = self.create_dict()

        expected_dict = {}
        for i in range(1000):
            expected_dict[i] = i * 100.0
            redis_dict[i] = i * 100.0

        items = list(redis_dict.scan_items())
        self.assertTrue(len(items) >= 1000)

        self.assertTrue(dict(items), expected_dict)

    def test_redis_version(self):
        redis_dict = self.create_dict()
        actual = '.'.join(str(c) for c in redis_dict.redis_version)
        expected = self.redis.info()['redis_version']
        self.assertEqual(actual, expected)

    @unittest.skipIf(sys.version_info < (3, 9), 'merge requires Python 3.9+')
    def test_merge_operator(self):
        orig_data = {'a': 0, 'b': 0}
        new_data = {'b': 1, 'c': 1}

        python_orig = orig_data.copy()
        python_new = new_data.copy()

        redis_orig = self.create_dict(orig_data)
        redis_new = self.create_dict(new_data)

        for orig, new in [
            (python_orig, python_new),
            (redis_orig, redis_new),
            (redis_orig, python_new),
            (python_orig, redis_new),
        ]:
            self.assertEqual(orig | new, {'a': 0, 'b': 1, 'c': 1})

    @unittest.skipIf(sys.version_info < (3, 9), 'merge requires Python 3.9+')
    def test_update_operator(self):
        d = self.create_dict({'a': 'b'})

        # built-in dicts
        d |= {'c': 42, 'x': 38}
        self.assertEqual(sorted(d.items()), [('a', 'b'), ('c', 42), ('x', 38)])

        # list of tuples
        d |= [('a', 'g')]
        self.assertEqual(sorted(d.items()), [('a', 'g'), ('c', 42), ('x', 38)])

        # Update from another redis_collections class
        redis_list = List([('a', 'h')], redis=self.redis)
        d |= redis_list
        self.assertEqual(sorted(d.items()), [('a', 'h'), ('c', 42), ('x', 38)])


    def test_hmset(self):
        d = self.create_dict(hmset_command='hset')
        if d.redis_version < (4, 0 , 0):
            self.skipTest('Test required redis >= 4.0.0')
        d.update({1: 2, 3: 4})


class CounterTest(RedisTestCase):
    def create_counter(self, *args, **kwargs):
        kwargs['redis'] = self.redis
        return Counter(*args, **kwargs)

    def test_set_get(self):
        for init in (self.create_counter, collections.Counter):
            c = init()
            c['a'] = 5
            self.assertEqual(c['a'], 5)

            # For whatever reason Python counters accept non-int values
            c['not integer'] = 'in fact string'
            self.assertEqual(c['not integer'], 'in fact string')

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
                ('f', 6),
                ('e', 5),
                ('d', 4),
                ('c', 3),
                ('b', 2),
                ('a', 1),
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

            # One Counter, one sequence, and kwargs
            c = init('abbccc')
            c.update(['a', 'a', 'b', 'b'], c=2)
            self.assertEqual(sorted(c.items()), expected_result)

            # One Counter, one redis_collections.List
            c = init('abbccc')
            redis_list = List(['a', 'a', 'b', 'b'], redis=self.redis)
            c.update(redis_list, c=2)
            self.assertEqual(sorted(c.items()), expected_result)

        # Writeback enabled
        c = self.create_counter(writeback=True)
        c[('tuple', 'key')] = 1
        self.assertIn(('tuple', 'key'), c._data())
        self.assertIn(('tuple', 'key'), c.cache)
        c.update({('tuple', 'key'): 2})
        self.assertEqual(c[('tuple', 'key')], 2)

    def _test_op(self, op, dicts_work=False):
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
        self.assertEqual(
            op(python_other, redis_counter), op(python_other, python_counter)
        )

        # Fail for non-counter types
        D = {'a': 2, 'b': 2, 'c': 2}
        if dicts_work:
            self.assertEqual(op(python_counter, D), op(redis_counter, D))
        else:
            for C in (redis_counter, python_counter):
                with self.assertRaises(TypeError):
                    op(C, D)

    def test_add(self):
        self._test_op(operator.add)

        result = self.create_counter('abbccc') + self.create_counter('aabbcc')
        self.assertTrue(isinstance(result, collections.Counter))
        self.assertEqual(result, {'a': 3, 'b': 4, 'c': 5})

    def test_sub(self):
        self._test_op(operator.sub)

        result = self.create_counter('abbccc') - self.create_counter('aabbcc')
        self.assertTrue(isinstance(result, collections.Counter))
        self.assertEqual(result, {'c': 1})

    @unittest.skipIf(sys.version_info >= (3, 9), 'Python 3.8 and below')
    def test_or(self):
        self._test_op(operator.or_, dicts_work=False)
        result = self.create_counter('abbccc') | self.create_counter('aabbcc')
        self.assertTrue(isinstance(result, collections.Counter))
        self.assertEqual(result, {'a': 2, 'b': 2, 'c': 3})

    @unittest.skipIf(sys.version_info < (3, 9), 'Python 3.9 and above')
    def test_or_three_nine(self):
        self._test_op(operator.or_, dicts_work=True)
        result = self.create_counter('abbccc') | self.create_counter('aabbcc')
        self.assertTrue(isinstance(result, collections.Counter))
        self.assertEqual(result, {'a': 2, 'b': 2, 'c': 3})

    def test_and(self):
        self._test_op(operator.and_)

        result = self.create_counter('abbccc') & self.create_counter('aabbcc')
        self.assertTrue(isinstance(result, collections.Counter))
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

    def test_pos(self):
        redis_counter = self.create_counter({'a': 1, 'b': -2, 'c': 3})
        python_counter = collections.Counter({'a': 1, 'b': -2, 'c': 3})

        self.assertEqual(+redis_counter, +python_counter)

    def test_neg(self):
        redis_counter = self.create_counter({'a': 1, 'b': -2, 'c': 3})
        python_counter = collections.Counter({'a': 1, 'b': -2, 'c': 3})

        self.assertEqual(-redis_counter, -python_counter)

    def test_with(self):
        with self.create_counter({'a': 1, 'b': 2}) as C:
            key = C.key
            self.assertEqual(C['a'], 1)
            self.assertEqual(C['b'], 2)

        with self.create_counter(key=key) as C:
            self.assertEqual(C['a'], 1)
            self.assertEqual(C['b'], 2)


class DefaultDictTest(RedisTestCase):
    def create_ddict(self, *args, **kwargs):
        kwargs['redis'] = self.redis
        return DefaultDict(*args, **kwargs)

    def test_init(self):
        for init in (self.create_ddict, collections.defaultdict):
            # None is an OK default_factory
            D = init()
            self.assertIsNone(D.default_factory)

            # A non-callable is not
            with self.assertRaises(TypeError):
                D = init('not callable')

            # Callables are OK
            D = init(int, {1: 2, 3: 4})
            self.assertEqual(D.default_factory, int)
            self.assertEqual(D[1], 2)
            self.assertEqual(D[3], 4)

        # Writeback is on for defaultdict
        D_1 = self.create_ddict()
        self.assertTrue(D_1.writeback)

        # Keywords are passed through
        D_2 = self.create_ddict(key=D_1.key)
        self.assertEqual(D_1.key, D_2.key)

    def test_None(self):
        # None is a special case - any misses get a KeyError
        for init in (self.create_ddict, collections.defaultdict):
            D = init()
            with self.assertRaises(KeyError):
                D['key']
            D['key'] = 1
            self.assertEqual(D['key'], 1)

    def test_set_get(self):
        redis_ddict = self.create_ddict(int)
        python_ddict = collections.defaultdict(int)

        self.assertEqual(redis_ddict['key_1'], python_ddict['key_1'])
        redis_ddict['key_1'] += 1
        python_ddict['key_1'] += 1
        self.assertEqual(redis_ddict['key_1'], python_ddict['key_1'])

        # Normal setting and getting should work too
        redis_ddict['key_1'] = 2
        python_ddict['key_1'] = 2
        self.assertEqual(redis_ddict['key_1'], python_ddict['key_1'])

    def test_with(self):
        with self.create_ddict(lambda: set()) as D:
            # Store a mutable value, modify it, and retrieve it - changes
            # should be reflected
            D['key'].add(1)
            D['key'].add(2)
            self.assertEqual(D['key'], {1, 2})

            # Changes are not in Redis yet
            self.assertEqual(D._data()['key'], set())

        # Closing the context manager syncs to Redis
        self.assertEqual(D._data()['key'], {1, 2})

    def test_copy(self):
        redis_ddict = self.create_ddict(lambda: 1)
        redis_copy = redis_ddict.copy()
        self.assertEqual(
            redis_ddict.default_factory, redis_copy.default_factory
        )


if __name__ == '__main__':
    unittest.main()
