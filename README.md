# Sparkify Data Pipeline using PostgreSQL

## About

Sparkify is a startup with a music streaming service.

This pipeline extracts data from the app to allow the Sparkify team
to analyse the user activity on the app and the music the users listen to.

The Sparkify team would like to be able to analyse:

- which songs the users are listening to.
- trending songs or most listened to songs.
- locations where users are.
- the level or subscription type of users.
- time of day when users are most active i.e. listening.

## Database schema

The database model is a star-schema design.

The star-schema allows for simpler and quicker queries since fact tables contain the data required for analysis without having to be joined to the dimension tables.

The star schema is easy to understand particularly for business people who may only be interested in the statistics or analytics rather than all the details about what is captured.

In this project, one can query the `songplays` table to get information about most played music, where it is being played from, the time when music is played, etc. All this can be done from the `songplays` table without querying the other tables or joining to them in queries.

Fact table(s):

- `songplays`.

Dimension tables:

- `artists` -- data about the artists.
- `songs` -- data about the songs.
- `time` -- data about the times when songs were played.
- `users` -- data about the users of the app.

## How to run
- `pip install -U -r requirements.txt` to install the Python dependencies preferably in a virtual environment.

- `python create_tables.py` to create the database tables.

- `python etl.py` to do the ETL.

If you can't be bothered to set up Postgres locally, you can have a go using Docker: 

```
docker run --name some-postgres \
  -p 5432:5432 \
  -e POSTGRES_PASSWORD=student \
  -e POSTGRES_USER=student \
  -e POSTGRES_DB=studentdb \
   postgres:12-alpine
```

## File Description

- `requirements.txt`
  This file contains the required Python packages and their exact versions
  so that the scripts run as expected anywhere.
  Run `pip install -r requirements.txt` to install the packages.

- `create_tables.py`

  This script contains the logic and queries for resetting the database by connecting to the database server, dropping (if it exists) and creating the `sparkifydb` database, dropping existing tables, and creating the tables anew.

  Run `python create_tables.py` to (re)create the `sparkifydb` database and its tables.

- `test.ipynb`
  Use this notebook to test that files have been processed and stored in the database.

- `sql_queries.py` contains the `SQL` queries that:

  - create required database tables
  - drop the database tables
  - insert data into the database tables
  - query for song and artist data from the database.

  The queries are referenced in `etl.py` and `create_tables.py`

- `etl.ipynb`
  This is an exploratory notebook for getting familiar with the data, transforming and testing the loading of data into the database.

- `etl.py`
  This script processes the data files and writes the data to the database.

  Run this script `python etl.py` to write the data from the data directory to the database.

- `dashboard.ipynb`
  This is a notebook showing charts of results of some of the queries that can be run on the database having run the `etl.py` script.

## Queries

- Artist and song title of the 10 most played songs.

```
SELECT
    songplays.artist_id,
    songplays.song_id,
    songs.title,
    artists.name,
    COUNT(songplays.*) As plays
FROM songplays
JOIN songs ON songs.song_id = songplays.song_id
JOIN artists ON songs.artist_id = artists.artist_id
GROUP BY songplays.artist_id, songplays.song_id, songs.title, artists.name
ORDER BY plays
LIMIT 10;
```

- Locations with more than 100 plays.

```
SELECT location, COUNT(*) AS count
FROM songplays
GROUP BY location
HAVING COUNT(*) > 100
ORDER BY count DESC
LIMIT 10;
```

- Most played song in each location.

```
SELECT
    name AS artist, title, ranking.location
FROM
    (
        SELECT
            location,
            artist_id,
            song_id,
            RANK () OVER (
                PARTITION BY artist_id, song_id
            )
        FROM songplays
        WHERE
            song_id IS NOT NULL
            AND
            artist_id IS NOT NULL
    ) AS ranking
JOIN songs USING (song_id)
JOIN artists ON ranking.artist_id = artists.artist_id;
```

- Time of the day with the most play count.

```
SELECT
    hour, COUNT(*) AS count
FROM time
GROUP BY hour
ORDER BY hour
```

- Number of users in each level (subscription type).

```
SELECT
    level, COUNT(*) AS count
FROM users
GROUP BY level;
```

- Song play count by level type (subscription type)

```
SELECT
    level, COUNT(*) AS count
FROM songplays
GROUP BY level
ORDER BY count DESC;
```

### Tools

- PostgreSQL
- Python

