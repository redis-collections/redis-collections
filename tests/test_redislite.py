import os
import unittest
from .base import RedisTestCase
dbfilename = None
try:
    import redislite
    dbfilename = 'redislite_test.rdb'
except ImportError:
    pass

class ListTest(RedisTestCase):
    dbfilename = dbfilename

    @unittest.skipIf(not dbfilename, "The redislite module is not installed")
    def test_redislite_pid(self):
        """
        The redislite server object should have a valid pid attribute
        """
        self.assertGreater(self.redis.pid, 0)

    @unittest.skipIf(not dbfilename, "The redislite module is not installed")
    def test_redislite_dbfile(self):
        """
        The db attribute on the redis object should match the dbfilename
        :return:
        """
        self.assertEqual(os.path.join(os.getcwd(), self.dbfilename), self.redis.db)
