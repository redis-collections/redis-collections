from __future__ import print_function, unicode_literals

from redis_collections.zcounter import ZCounter

from .base import RedisTestCase


class ZCounterTestCase(RedisTestCase):

    def create_zcounter(self, *args, **kwargs):
        kwargs['redis'] = self.redis
        return ZCounter(*args, **kwargs)

    def test_init(self):
        zc = self.create_zcounter()

    def test_contains(self):
        zc = self.create_zcounter()

        zc['member_1'] = 1
        self.assertIn('member_1', zc)

        self.assertNotIn('member_2', zc)

        del zc['member_1']
        self.assertNotIn('member_1', zc)

        zc[1] = 100
        self.assertNotIn(1.0, zc)

        zc[1.0] = 1000
        self.assertIn(1.0, zc)

    def test_delitem_index(self):
        self.fail()

    def test_delitem_slice(self):
        self.fail()

    def test_getitem(self):
        zc = self.create_zcounter()
        zc['member_1'] = 1
        zc['member_2'] = 2.0

        self.assertEqual(zc['member_1'], 1)
        self.assertEqual(zc['member_2'], 2.0)
        self.assertRaises(KeyError, lambda: zc['member_3'])

    def test_iter(self):
        self.fail()

    def test_len(self):
        zc = self.create_zcounter()

        self.assertEqual(len(zc), 0)

        zc['member_1'] = 1
        self.assertEqual(len(zc), 1)

        zc['member_2'] = 2.0
        self.assertEqual(len(zc), 2)

        del zc['member_1']
        self.assertEqual(len(zc), 1)

    def test_setitem(self):
        self.fail()

    def test_clear(self):
        self.fail()

    def test_copy(self):
        self.fail()

    def test_count(self):
        self.fail()

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
        self.fail()

    def test_update(self):
        self.fail()
