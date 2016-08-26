from __future__ import print_function, unicode_literals

from redis_collections.zcounter import ZCounter

from .base import RedisTestCase


class ZCounterTestCase(RedisTestCase):

    def create_zcounter(self, *args, **kwargs):
        kwargs['redis'] = self.redis
        return ZCounter(*args, **kwargs)

    def test_init(self):
        self.assertEqual(self.create_zcounter().items(), [])

        items = [('0', 1.0), ('1', 2.0)]
        self.assertEqual(self.create_zcounter(items).items(), items)

        data = {'0': 1.0, '1': 2.0}
        self.assertEqual(self.create_zcounter(data).items(), items)

    def test_contains(self):
        zc = self.create_zcounter()

        zc['member_1'] = 1
        self.assertIn('member_1', zc)

        self.assertNotIn('member_2', zc)

        del zc['member_1']
        self.assertNotIn('member_1', zc)

        # Unlike a Python dict or collections.Counter instance ZCounter
        # does not refuse to store numeric types like 1, 1.0, complex(1, 0)
        # in the same collection
        zc[1] = 100
        self.assertNotIn(1.0, zc)

        zc[1.0] = 1000
        self.assertIn(1.0, zc)

    def test_delitem(self):
        zc = self.create_zcounter()

        zc['member_1'] = 1
        self.assertIn('member_1', zc)

        del zc['member_1']
        self.assertNotIn('member_1', zc)

        with self.assertRaises(KeyError):
            del zc['member_1']

    def test_get_slice_del_slice(self):
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
            items = [
                ('0', 1), ('1', 2), ('2', 4), ('3', 8), ('4', 16), ('5', 32)
            ]
            zc = self.create_zcounter(items)

            index = slice(*slice_args)
            self.assertEqual(zc[index], items[index])

            del items[index]
            del zc[index]
            self.assertEqual(zc.items(), items)

    def test_getitem_setitem(self):
        zc = self.create_zcounter()
        zc['member_1'] = 1
        zc['member_2'] = 2.0

        self.assertEqual(zc['member_1'], 1)
        self.assertEqual(zc['member_2'], 2.0)
        self.assertRaises(KeyError, lambda: zc['member_3'])

    def test_iter(self):
        items = [('0', 1.0), ('1', 2.0)]
        zc = self.create_zcounter(items)

        self.assertEqual(tuple(zc.__iter__()), tuple(items))

    def test_len(self):
        zc = self.create_zcounter()

        self.assertEqual(len(zc), 0)

        zc['member_1'] = 1
        self.assertEqual(len(zc), 1)

        zc['member_2'] = 2.0
        self.assertEqual(len(zc), 2)

        del zc['member_1']
        self.assertEqual(len(zc), 1)

    def test_clear(self):
        zc = self.create_zcounter()

        zc['0'] = 1.0
        zc['1'] = 2.0
        self.assertEqual(zc.items(), [('0', 1.0), ('1', 2.0)])

        zc.clear()
        self.assertEqual(zc.items(), [])

    def test_copy(self):
        zc = self.create_zcounter()
        zc['0'] = 1.0
        zc['1'] = 2.0

        zc_2 = zc.copy()
        self.assertEqual(zc_2.items(), [('0', 1.0), ('1', 2.0)])
        self.assertTrue(zc.redis, zc_2.redis)

    def test_count_between(self):
        zc = self.create_zcounter()
        zc['0'] = 1.0
        zc['1'] = 2.0
        zc['2'] = 4.0
        zc['3'] = 8.0

        self.assertEqual(zc.count_between(), 4)
        self.assertEqual(zc.count_between(2.0), 3)
        self.assertEqual(zc.count_between(2.0, 4.0), 2)
        self.assertEqual(zc.count_between(4.0), 2)
        self.assertEqual(zc.count_between(8.0), 1)
        self.assertEqual(zc.count_between(0.0, 0.9), 0)
        self.assertEqual(zc.count_between(8.1), 0)
        self.assertEqual(zc.count_between(4.0, 2.0), 0)

    def test_get(self):
        zc = self.create_zcounter()
        zc['member_1'] = 1
        zc['member_2'] = 2.0

        self.assertEqual(zc.get('member_1'), 1)
        self.assertEqual(zc.get('member_2'), 2.0)
        self.assertEqual(zc.get('member_3', 0), 0)
        self.assertRaises(KeyError, lambda: zc['member_3'])

    def test_index(self):
        zc = self.create_zcounter()
        zc['member_1'] = 1
        zc['member_2'] = 2.0
        zc['member_3'] = 30.0

        self.assertEqual(zc.index('member_1'), 0)
        self.assertEqual(zc.index('member_2'), 1)
        self.assertEqual(zc.index('member_3'), 2)
        self.assertRaises(KeyError, lambda: zc.index('member_4'))

        self.assertRaises(KeyError, lambda: zc.index('member_4', reverse=True))
        self.assertEqual(zc.index('member_3', reverse=True), 0)
        self.assertEqual(zc.index('member_2', reverse=True), 1)
        self.assertEqual(zc.index('member_1', reverse=True), 2)

    def test_items(self):
        zc = self.create_zcounter()
        zc['0'] = 1.0
        zc['1'] = 2.0
        zc['2'] = 4.0
        zc['3'] = 8.0

        items = [('0', 1.0), ('1', 2.0), ('2', 4.0), ('3', 8.0)]

        self.assertEqual(zc.items(), items)
        self.assertEqual(zc.items(2.0), items[1:])
        self.assertEqual(zc.items(2.0, 4.0), items[1:3])
        self.assertEqual(zc.items(4.0), items[2:])
        self.assertEqual(zc.items(0.0, 0.9), [])
        self.assertEqual(zc.items(8.1), [])
        self.assertEqual(zc.items(4.0, 2.0), [])

        self.assertEqual(zc.items(reverse=True), items[::-1])
        self.assertEqual(zc.items(2.0, reverse=True), items[1:][::-1])
        self.assertEqual(zc.items(2.0, 4.0, reverse=True), items[1:3][::-1])
        self.assertEqual(zc.items(4.0, reverse=True), items[2:][::-1])
        self.assertEqual(zc.items(0.0, 0.9, reverse=True), [])
        self.assertEqual(zc.items(8.1, reverse=True), [])
        self.assertEqual(zc.items(4.0, 2.0, reverse=True), [])

    def test_update(self):
        zc = self.create_zcounter()
        zc['member_1'] = 0.0

        zc.update({'member_1': 1, 'member_2': 2.0})
        self.assertEqual(zc['member_1'], 1)
        self.assertEqual(zc['member_2'], 2.0)

        zc.update([('member_2', 20.0), ('member_3', 30.0)])
        self.assertEqual(zc['member_2'], 20.0)
        self.assertEqual(zc['member_3'], 30.0)

        zc_2 = self.create_zcounter()
        zc_2['member_3'] = 40.0
        zc.update(zc_2)
        self.assertEqual(zc['member_3'], 40.0)
