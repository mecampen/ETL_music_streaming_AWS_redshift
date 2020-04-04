import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events_table( 
        artist text,
        auth text,
        firstName text,
        gender text,
        ItemInSession int,
        lastName text,
        length float8,
        level text,
        location text,
        method text,
        page text,
        registration text,
        sessionId int,
        song text,
        status int,
        ts bigint,
        userAgent text,
        userId int
        )
        """
                              
)

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs_table( 
        num_songs text,
        artist_id text,
        artist_latitude text,
        artist_longitude text,
        artist_location text,
        song_id text,
        title text,
        duration text,
        year text
        )
        """
)

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay_table(
        songplay_id bigint IDENTITY(0,1),
        start_time text NOT NULL,
        user_id bigint NOT NULL,
        level text NOT NULL,
        song_id text NOT NULL,
        artist_id text NOT NULL,
        session_id bigint NOT NULL,
        location text NOT NULL,
        user_agent text NOT NULL
        )
        """
)

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user_table(
        user_id bigint NOT NULL,
        first_name text NOT NULL,
        last_name text NOT NULL,
        gender text NOT NULL,
        level text NOT NULL
    )
    """
)

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_table(
        song_id text NOT NULL,
        title text NOT NULL,
        artist_id text NOT NULL,
        year int NOT NULL,
        duration numeric NOT NULL
    )
    """
)

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist_table(
    artist_id text,
    name text NOT NULL,
    location text NOT NULL,
    lattitude text,
    longitude text
    )
    """
)

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time_table(
    start_time TIMESTAMP PRIMARY KEY,
    hour int NOT NULL,
    day int NOT NULL, 
    week int NOT NULL,
    month int NOT NULL,
    year int NOT NULL,
    weekday int NOT NULL
    )
    """
)

# STAGING TABLES

staging_events_copy=("""
    COPY staging_events_table FROM 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}' 
    region 'us-west-2' JSON 's3://udacity-dend/log_json_path.json';
""").format(config['IAM_ROLE']['ARN'])

staging_songs_copy=("""
    copy staging_songs_table from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off 
    JSON 'auto' truncatecolumns;
""").format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay_table(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT se.ts, se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
FROM staging_events_table as se
JOIN staging_songs_table as ss
ON se.song=ss.title
""")

user_table_insert = ("""
INSERT INTO user_table(user_id, first_name, last_name, gender, level)
SELECT DISTINCT se.userid, se.firstName, se.lastName, se.gender, se.level
FROM staging_events_table as se
WHERE se.userid IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO song_table(song_id, title, artist_id, year, duration)
SELECT ss.song_id, ss.title, ss.artist_id, CAST(ss.year as INT), CAST(ss.duration as numeric)
FROM staging_songs_table as ss
""")

artist_table_insert = ("""
INSERT INTO artist_table(artist_id, name, location, lattitude, longitude)
SELECT ss.artist_id, se.artist, ss.artist_location, ss.artist_latitude, ss.artist_longitude
FROM staging_songs_table as ss
JOIN staging_events_table as se
ON se.song=ss.title
""")

time_table_insert=("""
INSERT INTO time_table(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts, EXTRACT(HOUR FROM ts) as hour, EXTRACT(DAY FROM ts) as day, EXTRACT(WEEK FROM ts) as week,
EXTRACT(MONTH FROM ts) as month, EXTRACT(YEAR FROM ts) as year, EXTRACT(WEEKDAY FROM ts) as weekday
FROM(SELECT (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ') as ts FROM staging_events_table)""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
