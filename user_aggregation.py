import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import date, timedelta
from typing import Tuple, List


def data_download() -> Tuple[pd.DataFrame]:
    conns_cols = [
        "id",
        "installation_id",
        "connected_at",
        "is_internet_available",
        "is_protected",
        "captive_portal_mode",
        "signal_strength",
        "hotspot_id",
    ]

    hotspots_cols = [
        "id",
        "foursquare_id",
        "google_place_id",
        "created_at",
        "owner_id",
        "score_v4",
        "deleted_at",
    ]

    conns = pd.read_csv("data/conns_test.csv", usecols=conns_cols)
    hotspots = pd.read_csv("data/hotspots_test.csv", usecols=hotspots_cols)
    users = pd.read_csv("data/users_test.csv", usecols=["id", "email"])

    conns["connected_at"] = (
        conns["connected_at"].astype("datetime64[ns]").dt.normalize()
    )

    hotspots["created_at"] = (
        hotspots["created_at"].astype("datetime64[ns]").dt.normalize()
    )
    hotspots["deleted_at"] = (
        hotspots["deleted_at"].astype("datetime64[ns]").dt.normalize()
    )

    users["user"] = users.email.apply(lambda x: x.split("_")[0])
    users.drop(columns="email", inplace=True)

    return conns, hotspots, users


def merge_datasets(
    conns: pd.DataFrame, hotspots: pd.DataFrame, users: pd.DataFrame
) -> Tuple[pd.DataFrame]:
    """Merge downnloaded datasets

    Args:
        conns (pd.DataFrame): connections dataset
        hotspots (pd.DataFrame): hotspots dataset
        users (pd.DataFrame): users dataset

    Returns:
        Tuple[pd.DataFrame]: Returns a tuple of two desired datasets.
    """
    users_hotspots = (
        users[["id", "user"]]
        .merge(hotspots, how="inner", left_on="id", right_on="owner_id")
        .drop(columns=["id_x", "owner_id"])
        .rename(columns={"id_y": "id"})
    )

    users_conns = users_hotspots.merge(conns, left_on="id", right_on="hotspot_id")

    return users_hotspots, users_conns


def count_users_hotspots(users_hotspots: pd.DataFrame) -> pd.DataFrame:
    """Count the total number of created by user hotspots

    Args:
        users_hotspots (pd.DataFrame): users_hotspots dataset

    Returns:
        pd.DataFrame: Returns aggregated values user-amount of hotspots created
    """    
    return (
        users_hotspots.groupby("user", as_index=False)
        .agg({"id": "count"})
        .sort_values("id", ascending=False)
        .rename(columns={"id": "count"})
    )


def count_users_hotspots_geo(users_hotspots: pd.DataFrame) -> pd.DataFrame:
    """Count the total number of created by user hotspots with geoposition

    Args:
        users_hotspots (pd.DataFrame): users_hotspots dataset

    Returns:
        pd.DataFrame: Returns aggregated values user-amount of hotspots created
    """    
    return (
        users_hotspots[
            users_hotspots.foursquare_id.notnull()
            | users_hotspots.google_place_id.notnull()
        ]
        .groupby("user", as_index=False)
        .agg({"id": "count"})
        .sort_values("id", ascending=False)
        .rename(columns={"id": "count"})
    )


def count_users_hotspots_over_time(users_hotspots: pd.DataFrame) -> List[pd.DataFrame]:
    """Count the total number of created by user hotspots during various time periods

    Args:
        users_hotspots (pd.DataFrame): users_hotspots dataset

    Returns:
        List[pd.DataFrame]: Returns a list of aggregated values user-amount of hotspots created
    """
    result = []

    dates = (
        users_hotspots.created_at.min(),
        date.today() - pd.offsets.MonthBegin(1),
        pd.to_datetime(date.today() - timedelta(days=date.today().weekday() % 7)),
        pd.to_datetime(
            users_hotspots.created_at.max()
            - timedelta(days=users_hotspots.created_at.max().weekday())
        ),
    )

    for dt in dates:
        result.append(
            users_hotspots.loc[users_hotspots.created_at >= dt]
            .groupby("user", as_index=False)
            .agg({"id": "count"})
            .sort_values("id", ascending=False)
            .rename(columns={"id": "count"})
        )

    return result


def count_users_hotspots_score(users_hotspots: pd.DataFrame) -> pd.DataFrame:
    """Count the total number of created by user hotspots with desired score values

    Args:
        users_hotspots (pd.DataFrame): users_hotspots dataset

    Returns:
        pd.DataFrame: Returns aggregated values user-amount of hotspots created
    """    
    users_hotspots["good_hs"] = np.where(users_hotspots.score_v4 > 0.6, 1, 0)
    users_hotspots["avg_hs"] = np.where(
        (users_hotspots.score_v4 < 0.6) & (users_hotspots.score_v4 > 0.3), 1, 0
    )
    users_hotspots["bad_hs"] = np.where(users_hotspots.score_v4 < 0.3, 1, 0)

    return (
        users_hotspots.groupby("user", as_index=False)
        .agg({"good_hs": "sum", "avg_hs": "sum", "bad_hs": "sum"})
        .sort_values(by=["good_hs", "avg_hs", "bad_hs"], ascending=False)
    )


def count_users_unique_hotspots(
    users_conns: pd.DataFrame, users_hotspots: pd.DataFrame
) -> List[pd.DataFrame]:
    """count how many hotspots the user has to which there were more
    than 1, 5 and 10 unique connections during various time periods

    Args:
        users_conns (pd.DataFrame): users_conns dataset
        users_hotspots (pd.DataFrame): users_hotspots dataset

    Returns:
        List[pd.DataFrame]: Returns a list of aggregated values user-amount of hotspots
        unique connections (more then 1, more then 5 and more then 10)
    """
    result = []

    dates = (
        users_hotspots.created_at.min(),
        date.today() - pd.offsets.YearBegin(1),
        date.today() - pd.offsets.MonthBegin(1),
        pd.to_datetime(date.today() - timedelta(days=date.today().weekday() % 7)),
        pd.to_datetime(
            users_hotspots.created_at.max()
            - timedelta(days=users_hotspots.created_at.max().weekday() % 6)
        ),
    )

    for dt in dates:
        users_conns_unique = (
            users_conns.loc[(users_conns.connected_at >= dt)]
            .groupby(["user", "connected_at", "hotspot_id"], as_index=False)
            .agg({"installation_id": "count"})
            .groupby(["user", "hotspot_id"], as_index=False)
            .agg({"connected_at": "count"})
        )

        users_conns_unique["more_1_conns"] = np.where(
            users_conns_unique.connected_at > 1, 1, 0
        )
        users_conns_unique["more_5_conns"] = np.where(
            users_conns_unique.connected_at > 5, 1, 0
        )
        users_conns_unique["more_10_conns"] = np.where(
            users_conns_unique.connected_at > 10, 1, 0
        )

        result.append(
            users_conns_unique.groupby("user", as_index=False)
            .agg({"more_1_conns": "sum", "more_5_conns": "sum", "more_10_conns": "sum"})
            .sort_values(
                by=["more_1_conns", "more_5_conns", "more_10_conns"], ascending=False
            )
        )

    return result
