import json  # JSON encoder and decoder
import sqlite3  # SQL Interface for Python"
import datetime  # Manipulate date and time
import pandas as pd  # Python library used primarily for data analysis
import sqlalchemy  # SQL toolkit and ORM for SQL in Python
import requests  # HTTP Library for Python

DATABASE_LOCATION = "sqlite:///my_tracks.sqlite"
USER_ID = "user_name"
TOKEN = "AQAMMBHm9vR6_SWw6WLQBHzBYECchAUX75zgIabwyrz0bCDtGCnqaUPBSNH0OStwnpeHeZt-MKRPrrFtEyH77fxTTRje0BHwls9jgv34AYqebCeC0Gj-4ziFnZofkl6fTD9cDUieGo9ETTBP6XI5"  # Random Token


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs were downloaded. Finishing execution")
        return False

    # Check if primary key is valid
    if not pd.Series(df['played_at']).is_unique:
        raise Exception("Primary Key has duplicate values")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null data exists")

    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=4)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception(
                "At least one song does not come from within the last 24 hours")


if __name__ == "__main__":

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN),
    }

    today = datetime.datetime.now()  # Get the current time

    # Subtract one day today's date
    yesterday = today - datetime.timedelta(days=1)

    yesterday_unix_timestamp = int(
        yesterday.timestamp()) * 1000  # convert to milliseconds

    r = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?limit=50&after={yesterday}".format(
            yesterday=yesterday_unix_timestamp
        ),
        headers=headers
    )  # Make GET request to Spotify API

    # Make sure the request was OK
    r.raise_for_status()

    results = r.json()

    song_names = []
    artist_names = []
    played_at_times = []  # time song was played
    timestamps = []  # 24 hour period

    # Iterate through results and put it into an array
    for song in results["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["artists"][0]["name"])
        played_at_times.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    # Populate a dictionary to be used later in a data frame
    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_times,
        "timestamp": timestamps
    }

    # Create a data frame from the data stored in song_dict
    song_df = pd.DataFrame(song_dict, columns=[
                           "song_name", "artist_name", "played_at", "timestamp"], )

    # Validate data
    if check_if_valid_data(song_df):
        print("Data is valid, proceed to load the data")

    # Load the data
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('neil_tracks.sqlite')
    cursor = conn.cursor()

    sql_query_create = """
    CREATE TABLE IF NOT EXISTS neil_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """
    # Create table in SQL Database if it does not exist
    cursor.execute(sql_query_create)

    try:
        song_df.to_sql("neil_tracks", engine,
                       index=False, if_exists='append')
    except:
        print("Data already exists in the database!")

    print(song_df)
