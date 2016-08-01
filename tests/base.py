# -*- coding: utf-8 -*-
from __future__ import division, print_function, unicode_literals

import unittest

import redis


class RedisTestCase(unittest.TestCase):

    db = 15

    def setUp(self):
        self.redis = redis.StrictRedis(db=self.db)
        if self.redis.dbsize():
            raise EnvironmentError('Redis database number %d is not empty, '
                                   'tests could harm your data.' % self.db)

    def tearDown(self):
        self.redis.flushdb()
