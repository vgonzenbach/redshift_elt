from configparser import ConfigParser


# CONFIG
dwh_cfg = ConfigParser()
dwh_cfg.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist        VARCHAR(256),
    auth          VARCHAR(20),
    firstName     VARCHAR(256),
    gender        CHAR(1),
    itemInSession INTEGER,
    lastName      VARCHAR(256),
    length        FLOAT,
    level         VARCHAR(10),
    location      VARCHAR(256),
    method        VARCHAR(10),
    page          VARCHAR(50),
    registration  BIGINT,
    sessionId     INTEGER,
    song          VARCHAR(256),
    status        INTEGER,
    ts            BIGINT,
    userAgent     VARCHAR(512),
    userId        INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    artist_id        VARCHAR(19),
    artist_latitude  FLOAT,
    artist_location  VARCHAR(256),
    artist_longitude FLOAT,
    artist_name      VARCHAR(256),
    duration         FLOAT,
    num_songs        INTEGER,
    song_id          VARCHAR(19),
    title            VARCHAR(256),
    year             INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
  songplay_id BIGINT IDENTITY(0,1),
  start_time TIMESTAMP,
  user_id INT,
  level VARCHAR(4),
  song_id VARCHAR(19),
  artist_id VARCHAR(19),
  session_id INTEGER,
  location VARCHAR(256),
  user_agent VARCHAR(512)
);
""")

user_table_create = ("""
CREATE TABLE users (
  user_id INTEGER,
  first_name VARCHAR(20),
  last_name VARCHAR(15),
  gender CHAR(1),
  level VARCHAR(4)
);
""")

song_table_create = ("""
CREATE TABLE songs (
  song_id VARCHAR(19),
  title VARCHAR(256),
  artist_id VARCHAR(19),
  year SMALLINT,
  duration FLOAT8
);
""")

artist_table_create = ("""
CREATE TABLE artists (
  artist_id VARCHAR(19),
  name VARCHAR(256),
  location VARCHAR(256),
  latitude FLOAT4,
  longitude FLOAT4
);
""")

time_table_create = ("""
CREATE TABLE times (
  start_time TIMESTAMP sortkey,
  hour SMALLINT,
  day SMALLINT,
  week SMALLINT,
  month SMALLINT,
  year SMALLINT,
  weekday SMALLINT
);
""")

# STAGING TABLES

staging_events_copy = (f"""
COPY staging_events
FROM '{dwh_cfg['S3']['LOG_DATA']}'
IAM_ROLE '{dwh_cfg['IAM_ROLE']['ARN']}'
FORMAT AS JSON '{dwh_cfg['S3']['log_jsonpath']}';
""")

staging_songs_copy = (f"""
COPY staging_songs
FROM '{dwh_cfg['S3']['SONG_DATA']}'
IAM_ROLE '{dwh_cfg['IAM_ROLE']['ARN']}'
FORMAT AS JSON 'auto';
""")

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
	timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
	userid AS user_id,
    level,
    s.song_id AS song_id,
   	s.artist_id AS artist_id,
    sessionid AS session_id,
    location,
    useragent AS user_agent
FROM staging_events AS e
JOIN staging_songs AS s
	ON e.song = s.title
ORDER BY start_time;
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT userid::INTEGER , firstname, lastname, gender, level
FROM staging_events
WHERE TRIM(userid) != '';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT 
	DISTINCT song_id, 
    title,
    artist_id,
    year,
    duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT 
	DISTINCT artist_id, 
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO times(start_time, hour, day, week, month, year, weekday)
SELECT 
	timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
    EXTRACT(HOUR FROM start_time) AS hour,
    EXTRACT(DAY FROM start_time) AS day,
    EXTRACT(WEEK FROM start_time) AS week,
    EXTRACT(MONTH FROM start_time) AS month,
    EXTRACT(YEAR FROM start_time) AS year,
    EXTRACT(DOW FROM start_time) AS weekday
FROM staging_events
ORDER BY start_time;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
