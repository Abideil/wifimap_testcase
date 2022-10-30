import unittest

from user_aggregation import *


class TestCase(unittest.TestCase):
    def setUp(self):
        self.conns, self.hotspots, self.users = data_download()
        self.users_hotspots, self.users_conns = merge_datasets(
            self.conns, self.hotspots, self.users
        )

    def test_count_users_hotspots(self):
        message = "Amount of hotspots ids is not equal"
        self.assertEqual(
            self.users_hotspots.id.nunique(),
            count_users_hotspots(self.users_hotspots)["count"].sum(),
            message,
        )

    def test_count_users_hotspots_geo(self):
        message = "Amount of hotspots ids with geoposition is not equal"
        FirstValue = self.users_hotspots[
            self.users_hotspots.foursquare_id.notnull()
            | self.users_hotspots.google_place_id.notnull()
        ]["id"].nunique()
        SecondValue = count_users_hotspots_geo(self.users_hotspots)["count"].sum()
        self.assertEqual(FirstValue, SecondValue, message)

    def test_count_users_hotspots_over_time(self):
        dates = (
            self.users_hotspots.created_at.min(),
            date.today() - pd.offsets.MonthBegin(1),
            pd.to_datetime(date.today() - timedelta(days=date.today().weekday())),
        )
        self.assertEqual(
            self.users_hotspots.loc[self.users_hotspots.created_at >= dates[0]][
                "id"
            ].nunique(),
            count_users_hotspots_over_time(self.users_hotspots)[0]["count"].sum(),
        )
        self.assertEqual(
            self.users_hotspots.loc[self.users_hotspots.created_at >= dates[1]][
                "id"
            ].nunique(),
            count_users_hotspots_over_time(self.users_hotspots)[1]["count"].sum(),
        )
        self.assertEqual(
            self.users_hotspots.loc[self.users_hotspots.created_at >= dates[2]][
                "id"
            ].nunique(),
            count_users_hotspots_over_time(self.users_hotspots)[2]["count"].sum(),
        )

    def test_count_users_hotspots_score(self):
        count_users = count_users_hotspots_score(self.users_hotspots)
        self.assertEqual(
            count_users["good_hs"].sum(),
            self.users_hotspots.loc[self.users_hotspots.score_v4 > 0.6]["id"].nunique(),
        )
        self.assertEqual(
            count_users["avg_hs"].sum(),
            self.users_hotspots.loc[
                (self.users_hotspots.score_v4 < 0.6)
                & (self.users_hotspots.score_v4 > 0.3)
            ]["id"].nunique(),
        )
        self.assertEqual(
            count_users["bad_hs"].sum(),
            self.users_hotspots.loc[self.users_hotspots.score_v4 < 0.3]["id"].nunique(),
        )

    def test_count_users_unique_hotspots(self):
        dates = (
            self.users_hotspots.created_at.min(),
            date.today() - pd.offsets.YearBegin(1),
            date.today() - pd.offsets.MonthBegin(1),
            pd.to_datetime(date.today() - timedelta(days=date.today().weekday())),
            pd.to_datetime(
                self.users_hotspots.created_at.max()
                - timedelta(days=self.users_hotspots.created_at.max().weekday())
            ),
        )
        for dt in dates:
            FirstValue = (
                self.users_conns.loc[
                    (
                        self.users_conns.connected_at 
                        >= dt
                    )
                ]
                .groupby(["user", "connected_at", "hotspot_id"], as_index=False)
                .agg({"installation_id": "count"})
                .groupby(["user", "hotspot_id"], as_index=False)
                .agg({"connected_at": "count"})
            )
            
            SecondValue = (
                self.users_conns.loc[
                    (
                        self.users_conns.connected_at
                        >= dt
                    )
                ]
                .groupby(["user", "connected_at", "hotspot_id"], as_index=False)
                .agg({"installation_id": "count"})
            )

            self.assertEqual(
                FirstValue["connected_at"].sum(), SecondValue.installation_id.count()
            )

            self.assertEqual(
                self.users_conns.loc[
                    (
                        self.users_conns.connected_at 
                        >= dt
                    )
                ][
                    'id_y'
                ].nunique(), SecondValue.installation_id.sum()
            )


if __name__ == "__main__":
    unittest.main()
