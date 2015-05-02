# -*- coding: utf-8 -*-
use_redislite = False
try:
    import redislite
    use_redislite = True
except ImportError:
    import redis
import unittest



class RedisTestCase(unittest.TestCase):

    db = 15
    dbfilename = None

    @classmethod
    def setUpClass(cls):
        # If we're using redislite spin up a redis instance and keep it running while the class exists.
        # This keeps each test from starting a new redis-server and speeds things up quite a bit.
        if use_redislite:
            cls.redis_server = redislite.StrictRedis()
            cls.dbfilename = cls.redis_server.db

    def setUp(self):
        if use_redislite:
            self.redis = redislite.StrictRedis(self.dbfilename, db=self.db)
        else:
            self.redis = redis.StrictRedis(db=self.db)
        if self.redis.dbsize():
            raise EnvironmentError('Redis database number %d is not empty, '
                                   'tests could harm your data.' % self.db)

    def tearDown(self):
        self.redis.flushdb()
