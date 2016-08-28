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

    def test_repr(self):
        zc = self.create_zcounter([('zero', 0.0), ('one', 1.0)])
        repr_zc = repr(zc)
        self.assertIn("'zero': 0.0", repr_zc)
        self.assertIn("'one': 1.0", repr_zc)

    def test_contains(self):
        zc = self.create_zcounter()

        zc.set_score('member_1', 1)
        self.assertIn('member_1', zc)

        self.assertNotIn('member_2', zc)

        zc.discard_member('member_1')
        self.assertNotIn('member_1', zc)

        # Unlike a Python dict or collections.Counter instance ZCounter
        # does not refuse to store numeric types like 1, 1.0, complex(1, 0)
        # in the same collection
        zc.set_score(1, 100)
        self.assertNotIn(1.0, zc)

        zc.set_score(1.0, 1000)
        self.assertIn(1.0, zc)

    def test_iter(self):
        items = [('0', 1.0), ('1', 2.0)]
        zc = self.create_zcounter(items)

        self.assertEqual(tuple(zc.__iter__()), tuple(items))

    def test_len(self):
        zc = self.create_zcounter()

        self.assertEqual(len(zc), 0)

        zc.set_score('member_1', 1)
        self.assertEqual(len(zc), 1)

        zc.set_score('member_2', 2.0)
        self.assertEqual(len(zc), 2)

        zc.discard_member('member_1')
        self.assertEqual(len(zc), 1)

    def test_clear(self):
        zc = self.create_zcounter([('0', 1.0), ('1', 2.0)])
        self.assertEqual(zc.items(), [('0', 1.0), ('1', 2.0)])

        zc.clear()
        self.assertEqual(zc.items(), [])

    def test_copy(self):
        items = [('0', 1.0), ('1', 2.0)]
        zc = self.create_zcounter(items)

        zc_2 = zc.copy()
        self.assertEqual(zc_2.items(), items)
        self.assertTrue(zc.redis, zc_2.redis)

    def test_count_between(self):
        items = [('0', 1.0), ('1', 2.0), ('2', 4.0), ('3', 8.0)]
        zc = self.create_zcounter(items)

        self.assertEqual(zc.count_between(), 4)
        self.assertEqual(zc.count_between(2.0), 3)
        self.assertEqual(zc.count_between(2.0, 4.0), 2)
        self.assertEqual(zc.count_between(4.0), 2)
        self.assertEqual(zc.count_between(8.0), 1)
        self.assertEqual(zc.count_between(0.0, 0.9), 0)
        self.assertEqual(zc.count_between(8.1), 0)
        self.assertEqual(zc.count_between(4.0, 2.0), 0)

    def test_discard_between(self):
        items = [
            ('0', 1), ('1', 2), ('2', 4), ('3', 8), ('4', 16), ('5', 32)
        ]

        zc_1 = self.create_zcounter(items)
        zc_1.discard_between(min_rank=1)
        self.assertEqual(zc_1.items(), items[:1])

        zc_2 = self.create_zcounter(items)
        zc_2.discard_between(min_rank=1, max_rank=-2)
        self.assertEqual(zc_2.items(), [items[0], items[5]])

        zc_3 = self.create_zcounter(items)
        zc_3.discard_between(max_rank=-2)
        self.assertEqual(zc_3.items(), items[5:])

        zc_4 = self.create_zcounter(items)
        zc_4.discard_between(min_score=2)
        self.assertEqual(zc_4.items(), items[:1])

        zc_4 = self.create_zcounter(items)
        zc_4.discard_between(min_score=2, max_score=16)
        self.assertEqual(zc_4.items(), [items[0], items[5]])

        zc_5 = self.create_zcounter(items)
        zc_5.discard_between(max_score=16)
        self.assertEqual(zc_5.items(), items[5:])

        zc_6 = self.create_zcounter(items)
        zc_6.discard_between(min_rank=4, min_score=4)
        self.assertEqual(zc_6.items(), items[:2])

        zc_7 = self.create_zcounter(items)
        zc_7.discard_between(0, 1, 16, 32)
        self.assertEqual(zc_7.items(), items[2:4])

        zc_8 = self.create_zcounter(items)
        zc_8.discard_between()
        self.assertEqual(zc_8.items(), items)

    def test_discard_member(self):
        zc = self.create_zcounter()

        zc.set_score('member_1', 1)
        self.assertIn('member_1', zc)
        zc.discard_member('member_1')
        self.assertNotIn('member_1', zc)

        # No error for removing non-existient member
        zc.discard_member('member_1')

    def test_get(self):
        zc = self.create_zcounter([('member_1', 1), ('member_2', 2.0)])

        self.assertEqual(zc.get_score('member_1'), 1)
        self.assertEqual(zc.get_score('member_2'), 2.0)
        self.assertEqual(zc.get_score('member_3', 0), 0)
        self.assertEqual(zc.get_score('member_4'), None)

    def test_increment_score(self):
        zc = self.create_zcounter()

        zc.increment_score('member_1')
        self.assertEqual(zc.get_score('member_1'), 1.0)

        zc.increment_score('member_1', 1.0)
        self.assertEqual(zc.get_score('member_1'), 2.0)

        self.assertRaises(ValueError, zc.increment_score, 'member_1', '!')

    def test_get_rank(self):
        items = [('member_1', 1), ('member_2', 2.0), ('member_3', 30.0)]
        zc = self.create_zcounter(items)

        self.assertEqual(zc.get_rank('member_1'), 0)
        self.assertEqual(zc.get_rank('member_2'), 1)
        self.assertEqual(zc.get_rank('member_3'), 2)
        self.assertEqual(zc.get_rank('member_4'), None)

        self.assertEqual(zc.get_rank('member_4', reverse=True), None)
        self.assertEqual(zc.get_rank('member_3', reverse=True), 0)
        self.assertEqual(zc.get_rank('member_2', reverse=True), 1)
        self.assertEqual(zc.get_rank('member_1', reverse=True), 2)

    def test_items(self):
        items = [
            ('0', 1), ('1', 2), ('2', 4), ('3', 8), ('4', 16), ('5', 32)
        ]
        zc = self.create_zcounter(items)

        self.assertEqual(zc.items(), items[:])

        self.assertEqual(zc.items(min_rank=1), items[1:])
        self.assertEqual(zc.items(min_rank=1, max_rank=-2), items[1:-1])
        self.assertEqual(zc.items(max_rank=-2), items[:-1])
        self.assertEqual(
            zc.items(min_rank=1, max_rank=4, reverse=True), items[4:0:-1]
        )

        self.assertEqual(zc.items(min_score=4), items[2:])
        self.assertEqual(zc.items(min_score=4, max_score=16), items[2:-1])
        self.assertEqual(zc.items(max_score=4), items[:3])
        self.assertEqual(
            zc.items(min_score=2, max_score=16, reverse=True), items[4:0:-1]
        )

        self.assertEqual(zc.items(min_rank=1, min_score=4), items[2:])
        self.assertEqual(zc.items(min_rank=3, min_score=4), items[3:])
        self.assertEqual(zc.items(max_rank=4, min_score=4), items[2:5])
        self.assertEqual(zc.items(1, 4, 4, 8), items[2:4])
        self.assertEqual(zc.items(1, 4, 4, 8, reverse=True), items[3:1:-1])

    def test_update(self):
        zc = self.create_zcounter([('member_1', 0.0)])

        zc.update({'member_1': 1, 'member_2': 2.0})
        self.assertEqual(zc.get_score('member_1'), 1)
        self.assertEqual(zc.get_score('member_2'), 2.0)

        zc.update([('member_2', 20.0), ('member_3', 30.0)])
        self.assertEqual(zc.get_score('member_2'), 20.0)
        self.assertEqual(zc.get_score('member_3'), 30.0)

        zc_2 = self.create_zcounter()
        zc_2.set_score('member_3', 40.0)
        zc.update(zc_2)
        self.assertEqual(zc.get_score('member_3'), 40.0)
