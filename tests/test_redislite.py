import os
import sys
import unittest
from .base import RedisTestCase


class ListTest(RedisTestCase):
    dbfilename = 'redislite.rdb'
    @classmethod
    def setUpClass(cls):
        try:
            import redislite
        except ImportError:
            raise unittest.SkipTest('The redislite module is not installed')

    def test_redislite_pid(self):
        """
        The redislite server object should have a valid pid attribute
        """
        self.assertGreater(self.redis.pid, 0)

    def test_redislite_dbfile(self):
        """
        The db attribute on the redis object should match the dbfilename
        :return:
        """
        self.assertEqual(os.path.join(os.getcwd(), self.dbfilename), self.redis.db)

    @classmethod
    def tearDownClass(cls):
        if cls.dbfilename and os.path.exists(cls.dbfilename):
            os.remove(cls.dbfilename)