# -*- coding: utf-8 -*-


try:
    import redislite as redis
except ImportError:
    import redis
import unittest


class RedisTestCase(unittest.TestCase):

    db = 15
    dbfilename = None

    def setUp(self):
        if self.dbfilename and hasattr(redis, '__redis_executable__'):
            self.redis = redis.StrictRedis(self.dbfilename, db=self.db)
        else:
            self.redis = redis.StrictRedis(db=self.db)
        if self.redis.dbsize():
            raise EnvironmentError('Redis database number %d is not empty, '
                                   'tests could harm your data.' % self.db)

    def tearDown(self):
        self.redis.flushdb()
