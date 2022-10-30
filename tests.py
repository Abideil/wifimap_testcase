import unittest

from user_aggregation import *


class TestCase(unittest.TestCase):

    def setUp(self):
        self.conns, self.hotspots, self.users = data_download()
        self.users_hotspots, self.users_conns = merge_datasets(self.conns, self.hotspots, self.users)

    def test_count_users_hotspots(self):
        message = 'Amount of hotspots ids is not equal'
        self.assertEqual(self.users_hotspots.id.nunique(), count_users_hotspots(self.users_hotspots)['count'].sum(), message)

    def test_count_users_hotspots_geo(self):
        message = 'Amount of hotspots ids with geoposition is not equal'
        FirstValue = self.users_hotspots[
                        self.users_hotspots.foursquare_id.notnull()|
                        self.users_hotspots.google_place_id.notnull()
                    ]['id'].nunique()
        SecondValue = count_users_hotspots_geo(self.users_hotspots)['count'].sum()
        self.assertEqual(FirstValue, SecondValue, message)

    def test_count_users_hotspots_over_time(self):



if __name__ == '__main__':
    unittest.main()