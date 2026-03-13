
import os
import requests
from typing import Any

import boto3
import polars as pl
from dotenv import load_dotenv

API_URL = "https://api.songstats.com/enterprise/v1/artists/historic_stats"
ARTISTS = {
    "Bad Bunny": "xmcd3klh",
    "Bruno Mars": "2j0zuon6",
    "The Weeknd": "93b216mv",
}
BUCKET = "spotify-forecast"
LATEST_DATA = "latest_data.parquet"

load_dotenv()
s3 = boto3.client("s3")
ecs = boto3.client("ecs")


def load_songstats_data(artists: dict[str, str]) -> pl.DataFrame:
    print("Loading songstats data...")
    df_list = []
    for artist, songstats_id in artists.items():
        response = requests.get(
            API_URL,
            headers={"apikey": os.getenv("SONGSTATS_API_KEY")},
            params={
                "songstats_artist_id": songstats_id,
                "source": "spotify",
                "with_aggregates": "true",
                "start_date": "2020-06-01"  # before that the API behaves funky with respect to reach data
            }
        )

        df= pl.DataFrame(response.json()["stats"][0]["data"]["history"])
        df = df.with_columns(
            pl.col("date").str.to_date("%Y-%m-%d"),
            pl.lit(artist).alias("artist")
        )

        df_list.append(df)

    df = (pl.concat(df_list)
            .rename({
                "monthly_listeners_current": "monthly_listeners",
                "playlists_current": "playlists",
                "playlist_reach_current": "reach"
            })
            .sort(["date", "artist"])
            .filter(pl.col("monthly_listeners") > 0))

    df = fix_anomalies(df)

    # We interpret the values as lagged by one day
    return df.with_columns(
        pl.col("date") - pl.duration(days=1)
    )


def fix_anomalies(df: pl.DataFrame) -> pl.DataFrame:
    """
    Manually fix data anomalies.

    :param df: Dataframe to fix anomalies in
    :return: Dataframe with anomalies fixed
    """
    anomaly_mask = (((pl.col("artist") == "Bruno Mars") & (pl.col("date").is_between(pl.date(2026, 2, 15), pl.date(2026, 2, 16)))) |
                    ((pl.col("artist") == "Bad Bunny") & (pl.col("date") == pl.date(2021, 2, 16))))
    df = df.with_columns(
        pl.when(anomaly_mask).then(None).otherwise(pl.col("playlists")).alias("playlists"),
        pl.when(anomaly_mask).then(None).otherwise(pl.col("reach")).alias("reach"),
    )
    # Linear interpolation
    numeric_columns = [column for column, dtype in df.schema.items() if dtype.is_numeric()]
    return df.with_columns(
        pl.col(column).interpolate().over("artist")
        for column in numeric_columns
    )


def load_data() -> (pl.DataFrame, pl.DataFrame):
    """
    Loads and processes song statistics data, and a latest dataset
    containing the most recent data for each artist.

    :return: df_latest: dataset containing the most recent data for each artist
    """
    # df_spreadsheet = load_spreadsheet_data(ARTISTS.keys())  # TODO #11: is spreadsheet data sufficiently more up-to-date to justify additional complexity?
    df = load_songstats_data(ARTISTS).select(["date", "artist", "monthly_listeners", "reach"])

    df_latest = (df
                 .sort("date")
                 .with_columns(pl.all().fill_null(strategy="forward").over("artist"))
                 .group_by("artist")
                 .last())

    return df_latest


# ruff: noqa: ANN401 - typing for unused parameters is unimportant
def handler(_event: dict[str, Any], _context: Any) -> dict[str, Any]:
    """
    Trigger ECS task when date changes on the AAA and EIA websites are detected.

    :param _event: The event parameter passed by AWS Lambda. Ignored.
    :param _context: The context parameter passed by AWS Lambda. Ignored.
    :return: A dictionary containing:
             - statusCode (int): The HTTP status code indicating success or failure.
             - body (str): A message explaining the result of the execution.
    """
    df_latest = load_data().sort("artist")

    try:
        latest_s3_file = s3.get_object(Bucket=BUCKET, Key=LATEST_DATA)
        df_latest_s3 = pl.read_parquet(latest_s3_file["Body"]).sort("artist")
    except s3.exceptions.NoSuchKey:
        df_latest_s3 = pl.DataFrame()

    if not df_latest.equals(df_latest_s3):
        response = ecs.run_task(
            cluster="spotify-forecast-cluster",
            taskDefinition="spotify-forecast-task",
            launchType="FARGATE",
            networkConfiguration={
                "awsvpcConfiguration": {
                    "subnets": [
                        "subnet-08d81a899b03cad9e"
                    ],
                    "securityGroups": ["sg-09d197b8374bfc02b"],
                    "assignPublicIp": "ENABLED"
                }
            }
        )

        print(f"Started ECS task: {response['tasks'][0]['taskArn']}")
        return {"statusCode": 200, "body": "Change detected and ECS task started!"}

    print("No change detected.")
    return {"statusCode": 200, "body": "No changes"}
