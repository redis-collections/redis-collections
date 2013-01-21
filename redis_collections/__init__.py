

__title__ = 'redis_collections'
__version__ = '0.1.2'
__author__ = 'Jan Javorek'
__license__ = 'ISC'
__copyright__ = 'Copyright 2013 Jan Javorek'


from .base import RedisCollection

from .sets import Set
from .lists import List
from .dicts import Dict, Counter
