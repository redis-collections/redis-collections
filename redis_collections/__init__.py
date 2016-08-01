# -*- coding: utf-8 -*-
from __future__ import division, print_function, unicode_literals

__title__ = 'redis-collections'
__version__ = '0.1.8'
__author__ = 'Honza Javorek'
__license__ = 'ISC'
__copyright__ = 'Copyright 2013-? Honza Javorek'


from .base import RedisCollection  # NOQA

from .sets import Set  # NOQA
from .lists import List  # NOQA
from .dicts import Dict, Counter  # NOQA
