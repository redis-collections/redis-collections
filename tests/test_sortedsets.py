from redis_collections import GeoDB, SortedSetCounter

import six

from .base import RedisTestCase


class SortedSetCounterTestCase(RedisTestCase):

    def create_sortedset(self, *args, **kwargs):
        kwargs['redis'] = self.redis
        return SortedSetCounter(*args, **kwargs)

    def test_init(self):
        self.assertEqual(self.create_sortedset().items(), [])

        items = [('0', 1.0), ('1', 2.0)]
        self.assertEqual(self.create_sortedset(items).items(), items)

        data = {'0': 1.0, '1': 2.0}
        self.assertEqual(self.create_sortedset(data).items(), items)

    def test_repr(self):
        ssc = self.create_sortedset([('zero', 0.0), ('one', 1.0)])
        repr_ssc = repr(ssc)
        self.assertIn("'zero': 0.0", repr_ssc)
        self.assertIn("'one': 1.0", repr_ssc)

    def test_contains(self):
        ssc = self.create_sortedset()

        ssc.set_score('member_1', 1)
        self.assertIn('member_1', ssc)

        self.assertNotIn('member_2', ssc)

        ssc.discard_member('member_1')
        self.assertNotIn('member_1', ssc)

        # Unlike a Python dict or collections.Counter instance,
        # SortedSetCounter does not refuse to store numeric types like
        # 1, 1.0, complex(1, 0) in the same collection
        ssc.set_score(1, 100)
        self.assertNotIn(1.0, ssc)

        ssc.set_score(1.0, 1000)
        self.assertIn(1.0, ssc)

    def test_iter(self):
        items = [('0', 1.0), ('1', 2.0)]
        ssc = self.create_sortedset(items)

        self.assertEqual(tuple(ssc.__iter__()), tuple(items))

    def test_len(self):
        ssc = self.create_sortedset()

        self.assertEqual(len(ssc), 0)

        ssc.set_score('member_1', 1)
        self.assertEqual(len(ssc), 1)

        ssc.set_score('member_2', 2.0)
        self.assertEqual(len(ssc), 2)

        ssc.discard_member('member_1')
        self.assertEqual(len(ssc), 1)

    def test_clear(self):
        ssc = self.create_sortedset([('0', 1.0), ('1', 2.0)])
        self.assertEqual(ssc.items(), [('0', 1.0), ('1', 2.0)])

        ssc.clear()
        self.assertEqual(ssc.items(), [])

    def test_copy(self):
        items = [('0', 1.0), ('1', 2.0)]
        ssc_1 = self.create_sortedset(items)

        ssc_2 = ssc_1.copy()
        self.assertEqual(ssc_2.items(), items)
        self.assertTrue(ssc_1.redis, ssc_2.redis)

    def test_count_between(self):
        items = [('0', 1.0), ('1', 2.0), ('2', 4.0), ('3', 8.0)]
        ssc = self.create_sortedset(items)

        self.assertEqual(ssc.count_between(), 4)
        self.assertEqual(ssc.count_between(2.0), 3)
        self.assertEqual(ssc.count_between(2.0, 4.0), 2)
        self.assertEqual(ssc.count_between(4.0), 2)
        self.assertEqual(ssc.count_between(8.0), 1)
        self.assertEqual(ssc.count_between(0.0, 0.9), 0)
        self.assertEqual(ssc.count_between(8.1), 0)
        self.assertEqual(ssc.count_between(4.0, 2.0), 0)

    def test_discard_between(self):
        items = [
            ('0', 1), ('1', 2), ('2', 4), ('3', 8), ('4', 16), ('5', 32)
        ]

        ssc_1 = self.create_sortedset(items)
        ssc_1.discard_between(min_rank=1)
        self.assertEqual(ssc_1.items(), items[:1])

        ssc_2 = self.create_sortedset(items)
        ssc_2.discard_between(min_rank=1, max_rank=-2)
        self.assertEqual(ssc_2.items(), [items[0], items[5]])

        ssc_3 = self.create_sortedset(items)
        ssc_3.discard_between(max_rank=-2)
        self.assertEqual(ssc_3.items(), items[5:])

        ssc_4 = self.create_sortedset(items)
        ssc_4.discard_between(min_score=2)
        self.assertEqual(ssc_4.items(), items[:1])

        ssc_4 = self.create_sortedset(items)
        ssc_4.discard_between(min_score=2, max_score=16)
        self.assertEqual(ssc_4.items(), [items[0], items[5]])

        ssc_5 = self.create_sortedset(items)
        ssc_5.discard_between(max_score=16)
        self.assertEqual(ssc_5.items(), items[5:])

        ssc_6 = self.create_sortedset(items)
        ssc_6.discard_between(min_rank=4, min_score=4)
        self.assertEqual(ssc_6.items(), items[:2])

        ssc_7 = self.create_sortedset(items)
        ssc_7.discard_between(0, 1, 16, 32)
        self.assertEqual(ssc_7.items(), items[2:4])

        ssc_8 = self.create_sortedset(items)
        ssc_8.discard_between()
        self.assertEqual(ssc_8.items(), items)

    def test_discard_member(self):
        ssc = self.create_sortedset()

        ssc.set_score('member_1', 1)
        self.assertIn('member_1', ssc)
        ssc.discard_member('member_1')
        self.assertNotIn('member_1', ssc)

        # No error for removing non-existient member
        ssc.discard_member('member_1')

    def test_get_rank(self):
        items = [('member_1', 1), ('member_2', 2.0), ('member_3', 30.0)]
        ssc = self.create_sortedset(items)

        self.assertEqual(ssc.get_rank('member_1'), 0)
        self.assertEqual(ssc.get_rank('member_2'), 1)
        self.assertEqual(ssc.get_rank('member_3'), 2)
        self.assertEqual(ssc.get_rank('member_4'), None)

        self.assertEqual(ssc.get_rank('member_4', reverse=True), None)
        self.assertEqual(ssc.get_rank('member_3', reverse=True), 0)
        self.assertEqual(ssc.get_rank('member_2', reverse=True), 1)
        self.assertEqual(ssc.get_rank('member_1', reverse=True), 2)

    def test_get_score(self):
        ssc = self.create_sortedset([('member_1', 1), ('member_2', 2.0)])

        self.assertEqual(ssc.get_score('member_1'), 1)
        self.assertEqual(ssc.get_score('member_2'), 2.0)
        self.assertEqual(ssc.get_score('member_3', 0), 0)
        self.assertEqual(ssc.get_score('member_4'), None)

    def test_get_or_set_score(self):
        ssc = self.create_sortedset([('0', 0), ('1', 1)])

        self.assertEqual(ssc.get_or_set_score('0', 100), 0)
        self.assertEqual(ssc.get_score('0'), 0)

        self.assertEqual(ssc.get_or_set_score('2', 2), 2)
        self.assertEqual(ssc.get_score('2', 2), 2)

    def test_increment_score(self):
        ssc = self.create_sortedset()

        self.assertEqual(ssc.increment_score('member_1'), 1.0)
        self.assertEqual(ssc.get_score('member_1'), 1.0)

        self.assertEqual(ssc.increment_score('member_1', 1.0), 2.0)
        self.assertEqual(ssc.get_score('member_1'), 2.0)

        self.assertRaises(ValueError, ssc.increment_score, 'member_1', '!')

    def test_items(self):
        items = [
            ('0', 1), ('1', 2), ('2', 4), ('3', 8), ('4', 16), ('5', 32)
        ]
        ssc = self.create_sortedset(items)

        self.assertEqual(ssc.items(), items[:])

        self.assertEqual(ssc.items(min_rank=1), items[1:])
        self.assertEqual(ssc.items(min_rank=1, max_rank=-2), items[1:-1])
        self.assertEqual(ssc.items(max_rank=-2), items[:-1])
        self.assertEqual(
            ssc.items(min_rank=1, max_rank=4, reverse=True), items[4:0:-1]
        )

        self.assertEqual(ssc.items(min_score=4), items[2:])
        self.assertEqual(ssc.items(min_score=4, max_score=16), items[2:-1])
        self.assertEqual(ssc.items(max_score=4), items[:3])
        self.assertEqual(
            ssc.items(min_score=2, max_score=16, reverse=True), items[4:0:-1]
        )

        self.assertEqual(ssc.items(min_rank=1, min_score=4), items[2:])
        self.assertEqual(ssc.items(min_rank=3, min_score=4), items[3:])
        self.assertEqual(ssc.items(max_rank=4, min_score=4), items[2:5])
        self.assertEqual(ssc.items(1, 4, 4, 8), items[2:4])
        self.assertEqual(ssc.items(1, 4, 4, 8, reverse=True), items[3:1:-1])

    def test_scan_items(self):
        ssc = self.create_sortedset()

        expected_dict = {}
        for i in six.moves.range(1000):
            expected_dict[i] = i * 100.0
            ssc.set_score(i, i * 100.0)

        items = list(ssc.scan_items())
        self.assertTrue(len(items) >= 1000)

        self.assertTrue(dict(items), expected_dict)

    def test_update(self):
        ssc = self.create_sortedset([('member_1', 0.0)])

        ssc.update({'member_1': 1, 'member_2': 2.0})
        self.assertEqual(ssc.get_score('member_1'), 1)
        self.assertEqual(ssc.get_score('member_2'), 2.0)

        ssc.update([('member_2', 20.0), ('member_3', 30.0)])
        self.assertEqual(ssc.get_score('member_2'), 20.0)
        self.assertEqual(ssc.get_score('member_3'), 30.0)

        zc_2 = self.create_sortedset()
        zc_2.set_score('member_3', 40.0)
        ssc.update(zc_2)
        self.assertEqual(ssc.get_score('member_3'), 40.0)


class GeoDBTestCase(RedisTestCase):
    def create_geodb(self, *args, **kwargs):
        kwargs['redis'] = self.redis
        return GeoDB(*args, **kwargs)

    def test_getitem(self):
        geodb = self.create_geodb()
        geodb.set_location('St. Louis', 38.6270, -90.1994)

        actual = geodb['St. Louis']
        self.assertAlmostEqual(actual['latitude'], 38.6270, places=4)
        self.assertAlmostEqual(actual['longitude'], -90.1994, places=4)

    def test_setitem(self):
        geodb = self.create_geodb()
        geodb['St. Louis'] = {'latitude': 38.6270, 'longitude': -90.1994}

        actual = geodb['St. Louis']
        self.assertAlmostEqual(actual['latitude'], 38.6270, places=4)
        self.assertAlmostEqual(actual['longitude'], -90.1994, places=4)

        with self.assertRaises(KeyError):
            geodb['Bahia']

    def test_iter(self):
        geodb = self.create_geodb()
        geodb.set_location('St. Louis', 38.6270, -90.1994)
        geodb.set_location('Bahia', -11.4099, -41.2809)

        items = sorted(geodb, key=lambda x: x['place'])

        self.assertEqual(items[0]['place'], 'Bahia')
        self.assertAlmostEqual(items[0]['latitude'], -11.4099, places=4)
        self.assertAlmostEqual(items[0]['longitude'], -41.2809, places=4)

        self.assertEqual(items[1]['place'], 'St. Louis')
        self.assertAlmostEqual(items[1]['latitude'], 38.6270, places=4)
        self.assertAlmostEqual(items[1]['longitude'], -90.1994, places=4)

    def test_distance_between(self):
        geodb = self.create_geodb()
        geodb.set_location('St. Louis', 38.6270, -90.1994)
        geodb.set_location('Bahia', -11.4099, -41.2809)
        geodb.set_location('Berlin', 52.5200, 13.4050)
        geodb.set_location('Sydney', -33.8562, 151.2153)

        for place_1, place_2, expected, unit in [
            ('St. Louis', 'Bahia', 7528, 'km'),
            ('Bahia', 'St. Louis', 4677, 'mi'),
            ('St. Louis', 'Berlin', 24611784, 'ft'),
            ('St. Louis', 'Sydney', 14588620, 'm'),
        ]:
            actual = geodb.distance_between(place_1, place_2, unit=unit)
            self.assertAlmostEqual(actual, expected, delta=1)

        # Missing item returns None
        self.assertIsNone(geodb.distance_between('St. Louis', 'y'))
        self.assertIsNone(geodb.distance_between('x', 'St. Louis'))
        self.assertIsNone(geodb.distance_between('x', 'y'))

    def test_get_hash(self):
        geodb = self.create_geodb()
        geodb.set_location('St. Louis', 38.6270, -90.1994)
        self.assertEqual(geodb.get_hash('St. Louis'), '9yzgeryf9d0')

    def test_get_set_location(self):
        geodb = self.create_geodb()
        geodb.set_location('St. Louis', 38.6270, -90.1994)

        response = geodb.get_location('St. Louis')
        self.assertAlmostEqual(response['latitude'], 38.6270, places=4)
        self.assertAlmostEqual(response['longitude'], -90.1994, places=4)

        self.assertIsNone(geodb.get_location('x'))

    def test_places_within_radius(self):
        geodb = self.create_geodb()
        geodb.set_location('St. Louis', 38.6270, -90.1994)
        geodb.set_location('Bahia', -11.4099, -41.2809)
        geodb.set_location('Berlin', 52.5200, 13.4050)
        geodb.set_location('Sydney', -33.8562, 151.2153)

        # By default the results are sorted from nearest to farthest
        response = geodb.places_within_radius(place='St. Louis', radius=7530)
        self.assertEqual(response[1]['place'], 'Berlin')
        self.assertAlmostEqual(response[1]['latitude'], 52.5200, places=4)
        self.assertAlmostEqual(response[1]['longitude'], 13.4050, places=4)
        self.assertAlmostEqual(response[1]['distance'], 7501, delta=1)
        self.assertEqual(response[1]['unit'], 'km')

        # Test latitude & longitude, units
        response = geodb.places_within_radius(
            latitude=38.6, longitude=-90.2, radius=100, unit='mi',
        )
        self.assertEqual(response[0]['place'], 'St. Louis')

        # Test sort descending
        response = geodb.places_within_radius(
            place='St. Louis', radius=7530, sort='DESC'
        )
        self.assertEqual(response[0]['place'], 'Bahia')

    def test_update(self):
        geodb_1 = self.create_geodb()
        geodb_1.set_location('St. Louis', 38.6270, -90.1994)

        geodb_2 = self.create_geodb()
        geodb_2.set_location('Bahia', -11.4099, -41.2809)
        geodb_2.set_location('Berlin', 52.5200, 13.4050)

        # Update geodb_1 with geodb_2
        geodb_1.update(geodb_2)

        response = geodb_1.get_location('Bahia')
        self.assertAlmostEqual(response['latitude'], -11.4099, places=4)
        self.assertAlmostEqual(response['longitude'], -41.2809, places=4)

        response = geodb_1.get_location('Berlin')
        self.assertAlmostEqual(response['latitude'], 52.5200, places=4)
        self.assertAlmostEqual(response['longitude'], 13.4050, places=4)

        # Update geodb_3 with a dict
        geodb_3 = self.create_geodb()
        geodb_3.update(
            {
                'St. Louis': {'latitude': 38.6270, 'longitude': -90.1994},
                'Sydney': {'latitude': -33.8562, 'longitude': 151.2153},
            }
        )
        response = geodb_3.get_location('Sydney')
        self.assertAlmostEqual(response['latitude'], -33.8562, places=4)
        self.assertAlmostEqual(response['longitude'], 151.2153, places=4)

        # Update geodb_3 with a list
        geodb_3.update(
            [
                ('Bahia', -11.4099, -41.2809),
                ('Berlin', 52.5200, 13.4050),
            ]
        )
        response = geodb_3.get_location('Bahia')
        self.assertAlmostEqual(response['latitude'], -11.4099, places=4)
        self.assertAlmostEqual(response['longitude'], -41.2809, places=4)
