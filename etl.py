import logging
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s %(levelname)s] %(message)s",
    datefmt="%d %b %Y %H:%M:%S",
)


def process_song_file(cur, filepath):
    """
    - Reads the file contents whose path is `filepath` into a Pandas dataframe.

    - Extracts the fields required for the `songs` table from the dataframe.

    - Writes the song data to the `songs` table using the `song_table_insert` query
    and the database cursor `cur`.

    - Extracts the fields required for the `artists` table from the dataframe.

    - Writes the artists data to the `artists` table using the `artist_table_insert` query
    and the database cursor `cur`.
    """
    # open song file
    logging.debug("Reading song file: %s", filepath)
    df = pd.read_json(filepath, lines=True)

    # insert song record
    logging.debug("Song table insert")
    song_data = list(
        df[["song_id", "title", "artist_id", "year", "duration"]].values[0]
    )
    cur.execute(song_table_insert, song_data)

    # insert artist record
    logging.debug("Artist table insert")
    artist_data = list(
        df[
            [
                "artist_id",
                "artist_name",
                "artist_location",
                "artist_latitude",
                "artist_longitude",
            ]
        ].values[0]
    )
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Reads the content of the logfile at `filepath` into a dataframe.

    - Filters the dataframe for rows whose `page` value is `NextSong`.

    - Converts the timestamp column to datatime.

    - Builds data for the `time` table and inserts it into the table
    using the `time_table_insert` query and the database cursor `cur`.

    - Extracts user data from the dataframe and writes it to the database
    using the `user_table_insert` query and the database cursor `cur`.

    - Loops through the dataframe, searches for the song title and artist
    using the `song_select` query and writes the song play data into the
    `songplay_table_insert` query and the database cursor `cur`.

    -
    """
    # open log file
    logging.debug("Reading log file: %s", filepath)
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    logging.debug("Filtering logfile on `page == NextSong`")
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")

    # insert time data records
    time_data = (
        t,
        t.dt.hour,
        t.dt.day,
        # t.dt.week,
        t.dt.isocalendar().week,
        t.dt.month,
        t.dt.year,
        t.dt.weekday,
    )
    column_labels = (
        "timestamp",
        "hour",
        "day",
        "week_of_year",
        "month",
        "year",
        "weekday",
    )
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    logging.debug("Time table insert")
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    logging.debug("User table insert")
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    logging.debug("Song play insert")
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            pd.to_datetime(row.ts, unit="ms"),
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    logging.info("%s files found in %s", num_files, filepath)

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        logging.info("%s/%s files processed.", i, num_files)


def main():
    """
    - Connects to the database.

    - Processes song data in the `data/song_data` directory using the `process_song_file` function.

    - Processes log data in the `data/log_data` directory using the `process_log_file` function.

    - Closes the database connection.
    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
