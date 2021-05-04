top_10_songs_query = """
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
"""

top_10_locations_query = """
SELECT location, COUNT(*) AS count
FROM songplays
GROUP BY location
HAVING COUNT(*) > 100
ORDER BY count DESC
LIMIT 10;
"""

most_frequent_time_of_day_query = """
SELECT
    hour, COUNT(*) AS count
FROM time
GROUP BY hour
ORDER BY hour
"""

users_in_level_query = """
SELECT
    level, COUNT(*) AS count
FROM users
GROUP BY level
"""
