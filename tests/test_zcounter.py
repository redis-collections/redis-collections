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

        zc[1] = 100
        self.assertIn(1, zc)
        self.assertIn(1.0, zc)
        self.assertIn(complex(1, 0), zc)

    def test_delitem_index(self):
        self.fail()

    def test_delitem_slice(self):
        self.fail()

    def test_getitem_rank(self):
        self.fail()

    def test_getitem_slice(self):
        self.fail()

    def test_iter(self):
        self.fail()

    def test_len(self):
        self.fail()

    def test_setitem(self):
        self.fail()

    def test_clear(self):
        self.fail()

    def test_copy(self):
        self.fail()

    def test_count(self):
        self.fail()

    def test_index(self):
        self.fail()

    def test_items(self):
        self.fail()

    def test_update(self):
        self.fail()
