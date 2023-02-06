import os
import unittest

import redis


REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')


class RedisTestCase(unittest.TestCase):
    db = 15

    def setUp(self):
        self.redis = redis.StrictRedis.from_url(
            'redis://{}:{}'.format(REDIS_HOST, REDIS_PORT), db=self.db
        )
        if self.redis.dbsize():
            raise EnvironmentError(
                'Redis database number %d is not empty, '
                'tests could harm your data.' % self.db
            )

    def tearDown(self):
        self.redis.flushdb()
