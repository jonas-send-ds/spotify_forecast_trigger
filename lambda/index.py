
import os
import requests
from typing import Iterable
from typing import Any

import boto3
import gspread
import polars as pl
from dotenv import load_dotenv

# TODO #15: move to pipeline repo to make coupling explicit and avoid copied code

API_URL = "https://api.songstats.com/enterprise/v1/artists/historic_stats"
ARTISTS = {
    "Bad Bunny": "xmcd3klh",
    "Bruno Mars": "2j0zuon6",
    "The Weeknd": "93b216mv",
}
BUCKET = "spotify-forecast"
COLUMNS = ["artist", "date", "monthly_listeners", "reach"]
# TODO: find better way to handle this
GOOGLE_CREDENTIALS = {
  "type": "service_account",
  "project_id": "david-spotify-forecast",
  "private_key_id": "96c5bb633292dcbc6d7f0a0552483c21325f5de9",
  "client_email": "spotify-forecast-account@david-spotify-forecast.iam.gserviceaccount.com",
  "client_id": "117941236911552114013",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/spotify-forecast-account%40david-spotify-forecast.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}
LATEST_DATA = "latest_data.parquet"
PRIVATE_KEY_IS_NOT_SET = "GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY environment variable is not set"
SPREADSHEET_ID = "167UTVu2XVAM0MlGw-Cpw0tcMyuphC3ifJpOiC_y_a74"

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

    # We interpret the monthly listeners values as lagged by one day
    return df.with_columns(
        pl.col("monthly_listeners").shift(-1).over("artist")
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


def get_credentials() -> dict:
    """
    Retrieve the Google service account credentials.

    :return: A dictionary containing the credentials-data.
    """
    print("Loading Google Service Account credentials...")
    credentials = GOOGLE_CREDENTIALS.copy()

    load_dotenv()
    private_key_env = os.getenv("GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY")

    if not private_key_env:
        raise ValueError(PRIVATE_KEY_IS_NOT_SET)

    # HACKY: Replace literal \n with actual newline characters
    credentials["private_key"] = private_key_env.replace("\\n", "\n")

    return credentials


def load_spreadsheet_data(artists: Iterable[str]) -> pl.DataFrame:
    """
    Loads and processes data from a Google Spreadsheet containing monthly listener
    statistics for specified artists.

    :param artists: A list of artist names to select specific columns from the spreadsheet.
    :return: A Polars DataFrame containing the processed data with the following
        columns: date, artist, and monthly_listeners. The DataFrame is sorted by
        date and artist, with missing values removed.
    """
    credentials = get_credentials()
    client = gspread.service_account_from_dict(credentials)

    print("Loading and processing data from Google Spreadsheet...")
    public_sheet: gspread.Spreadsheet = client.open_by_key(SPREADSHEET_ID)
    worksheet = public_sheet.worksheet("Monthly Listeners")
    data = worksheet.get_all_values()  # list of lists

    df = pl.DataFrame(
        data[1:],
        schema=data[0],  # first row as column names
        orient="row",
    )

    return (df.select(["Date", *[x for x in artists]])
            .rename({"Date": "date"})
            .unpivot(index="date", variable_name="artist", value_name="monthly_listeners")
            .with_columns(
                pl.col("date").str.to_date("%m/%d/%Y"),
                pl.col("monthly_listeners").str.replace_all(",", "").replace("", None).cast(pl.Int64))
            .drop_nulls()
            .sort(["date", "artist"]))


def load_data() -> pl.DataFrame:
    """
    Loads and processes a latest dataset containing the most recent data for each artist.

    :return: df_latest: dataset containing the most recent data for each artist
    """
    df_songstats = load_songstats_data(ARTISTS).select(["date", "artist", "monthly_listeners", "reach"])
    df_spreadsheet = load_spreadsheet_data(ARTISTS.keys())

    df = pl.concat([df_songstats, df_spreadsheet], how="diagonal_relaxed")

    # use monthly listeners data from spreadsheet (when available) and songstats data for reach
    # TODO: make more robust to sorting of dataframes
    df = (df.group_by(["date", "artist"]).agg(
        pl.col("monthly_listeners").last(),
        pl.col("reach").first(),
    ).sort(["date", "artist"]))

    return (df
                 .sort("date")
                 .with_columns(pl.all().fill_null(strategy="forward").over("artist"))
                 .group_by("artist")
                 .last())


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
    df_latest = load_data().select(COLUMNS).sort("artist")

    try:
        latest_s3_file = s3.get_object(Bucket=BUCKET, Key=LATEST_DATA)
        df_latest_s3 = pl.read_parquet(latest_s3_file["Body"]).select(COLUMNS).sort("artist")
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
