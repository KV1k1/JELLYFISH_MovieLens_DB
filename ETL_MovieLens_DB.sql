--              Vytvorenie (ak neexistuje) a používanie databázy a schémy pre staging tabuľky
CREATE DATABASE IF NOT EXISTS JELLYFISH_MovieLens_DB;
CREATE SCHEMA IF NOT EXISTS JELLYFISH_MovieLens_DB.staging;

USE DATABASE JELLYFISH_MovieLens_DB;
USE SCHEMA JELLYFISH_MovieLens_DB.staging;
USE ROLE TRAINING_ROLE;

--                                      Vytvorenie staging tabuliek

-- age_group_staging
CREATE TABLE age_group_staging (
    id INT PRIMARY KEY,
    name VARCHAR(45)
);

-- genres_staging
CREATE TABLE genres_staging(
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

-- movies_staging
CREATE TABLE movies_staging(
    id INT PRIMARY KEY,
    title VARCHAR(255),
    release_year CHAR(4)
);

-- genres_movies_staging
CREATE TABLE genres_movies_staging(
    id INT PRIMARY KEY,
    movie_id INT,
    genre_id INT,
    FOREIGN KEY (movie_id) REFERENCES movies_staging(id),
    FOREIGN KEY (genre_id) REFERENCES genres_staging(id)
);

-- occupations_staging
CREATE TABLE occupations_staging(
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

-- users_staging
CREATE TABLE users_staging(
    id INT PRIMARY KEY,
    age INT,
    gender CHAR(1),
    zip_code VARCHAR(255),
    occupation_id INT,
   -- userscol VARCHAR(45),
    FOREIGN KEY (occupation_id) REFERENCES occupations_staging(id),
    FOREIGN KEY (age) REFERENCES age_group_staging(id)
);

-- ratings_staging
CREATE TABLE ratings_staging(
    id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    rating INT,
    rated_at TIMESTAMP_NTZ,
    FOREIGN KEY (user_id) REFERENCES users_staging(id),
    FOREIGN KEY (movie_id) REFERENCES movies_staging(id)
);

-- tags_staging
CREATE TABLE tags_staging(
    id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    tags VARCHAR(4000),
    created_at TIMESTAMP_NTZ,
    FOREIGN KEY (user_id) REFERENCES users_staging(id),
    FOREIGN KEY (movie_id) REFERENCES movies_staging(id)
);


--                                      Vytvorenie my_stage pre .csv súbory
CREATE OR REPLACE STAGE MovieLens_Stage;


COPY INTO age_group_staging
FROM @MovieLens_Stage/age_group.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
-- ON_ERROR = 'CONTINUE';

COPY INTO genres_staging
FROM @MovieLens_Stage/genres.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
-- ON_ERROR = 'CONTINUE';

COPY INTO movies_staging
FROM @MovieLens_Stage/movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
-- ON_ERROR = 'CONTINUE';

COPY INTO genres_movies_staging
FROM @MovieLens_Stage/genres_movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
-- ON_ERROR = 'CONTINUE'; 

COPY INTO occupations_staging
FROM @MovieLens_Stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
-- ON_ERROR = 'CONTINUE';

COPY INTO users_staging
FROM @MovieLens_Stage/users.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
-- ON_ERROR = 'CONTINUE';

COPY INTO ratings_staging
FROM @MovieLens_Stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
--ON_ERROR = 'CONTINUE';

COPY INTO tags_staging
FROM @MovieLens_Stage/tags.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
-- ON_ERROR = 'CONTINUE';


--                                      ELT - (T)ransform

-- dim_genres
CREATE OR REPLACE TABLE dim_genres AS
SELECT
    id AS dim_genreID,
    name AS genre_name,
FROM genres_staging g;
-- drop table dim_genres;
-- select * from dim_genres order by dim_genreID;
 
-- dim_movies
CREATE OR REPLACE TABLE dim_movies AS (
SELECT
    m.id AS dim_movieID,
    m.title,
    m.release_year,
    (                       -- comma seperated genres
        SELECT LISTAGG(DISTINCT g.name, ', ') WITHIN GROUP (ORDER BY g.name)
        FROM genres_movies_staging gm
        JOIN genres_staging g ON gm.genre_id = g.id
        WHERE gm.movie_id = m.id
    ) AS associated_genres,
    (                       -- first genre for a movie      
        SELECT MIN(g.name)
        FROM genres_movies_staging gm
        JOIN genres_staging g ON gm.genre_id = g.id
        WHERE gm.movie_id = m.id
    ) AS main_genre,
    (                       -- comma separated tags
        SELECT LISTAGG(DISTINCT t.tags, ', ') WITHIN GROUP (ORDER BY t.tags)
        FROM tags_staging t
        WHERE t.movie_id = m.id
    ) AS associated_tags,
    (                       -- first tag
        SELECT MIN(t.tags)
        FROM tags_staging t
        WHERE t.movie_id = m.id
    ) AS first_tag
FROM movies_staging m
);
-- drop table dim_movies;
-- select * from dim_movies order by dim_movieid asc;

-- dim_tags
CREATE OR REPLACE TABLE dim_tags AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY t.tags) AS dim_tagID,
    t.tags AS tag_name,
    MIN(t.created_at) AS created_at,
    COUNT(DISTINCT t.movie_id) AS tag_usage_count, -- specific tag usage
FROM tags_staging t
LEFT JOIN ratings_staging r ON t.movie_id = r.movie_id
GROUP BY t.tags;

-- drop table dim_tags;
-- select * from dim_tags where tag_name LIKE 'charming' order by dim_tagID;

-- dim_users
CREATE OR REPLACE TABLE dim_users AS
SELECT
    u.id AS dim_userID,
    ag.name AS age_group,
    u.gender,
    u.zip_code,
    o.name AS occupation
FROM users_staging u
JOIN age_group_staging ag ON u.age = ag.id
JOIN occupations_staging o ON u.occupation_id = o.id;

-- drop table dim_users;
-- select * from dim_users order by dim_userid asc;


-- dim_dates
CREATE TABLE dim_dates AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE)) AS dim_dateID,
    CAST(rated_at AS DATE) AS rated_at,
    DATE_PART('day', rated_at) AS day,
    DATE_PART('dow', rated_at) + 1 AS day_of_week,
    CASE DATE_PART('dow', rated_at) + 1
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_of_week_string,
    DATE_PART('week', rated_at) AS week,
    DATE_PART('month', rated_at) AS month,
    CASE DATE_PART('month', rated_at)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_string,
    DATE_PART('year', rated_at) AS year,
    DATE_PART('quarter', rated_at) AS quarter
FROM ratings_staging
GROUP BY CAST(rated_at AS DATE), 
         DATE_PART(day, rated_at), 
         DATE_PART(dow, rated_at), 
         DATE_PART(month, rated_at), 
         DATE_PART(year, rated_at), 
         DATE_PART(week, rated_at), 
         DATE_PART(quarter, rated_at);
-- drop table dim_dates;
-- select * from dim_dates where year = 2000;


-- dim_time
CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY rated_at) AS dim_timeID,
    CAST(rated_at AS TIME) AS time,
    DATE_PART('hour', time) AS hour,
    DATE_PART('minute', time) AS minute,
    DATE_PART('second', time) AS second,
    CASE
        WHEN DATE_PART('hour', time) < 12 THEN 'AM'
        ELSE 'PM'
    END AS ampm
FROM 
    (SELECT DISTINCT CAST(rated_at AS TIME) AS rated_at FROM ratings_staging);

-- drop table dim_time;
-- select * from dim_time order by dim_timeid;

-- fact_ratings
CREATE OR REPLACE TABLE fact_ratings AS
SELECT
    r.id AS fact_ratingID,                                  -- rating ID
    r.rated_at AS rating_datetime,                          -- datetime
    r.rating AS rating,                                     -- user rating
    du.dim_userID AS dim_userID,                            -- user dim ID
    dm.dim_movieID AS dim_movieID,                          -- movie dim ID
    dd.dim_dateID AS dim_dateID,                            -- date dim ID
    dt.dim_timeID AS dim_timeID,                            -- time dim ID
    dg.dim_genreID AS dim_genreID,                          -- genre dim ID
    dtg.dim_tagID AS dim_tagID,                             -- tag dim ID
    
    (           -- total number of ratings for the movie
        SELECT COUNT(*)
        FROM ratings_staging
        WHERE movie_id = r.movie_id
    ) AS num_of_movie_ratings,               
    
    (           -- avg movie rating
        SELECT ROUND(AVG(rating),2)
        FROM ratings_staging
        WHERE movie_id = r.movie_id
    ) AS avg_movie_rating,
     
    (           -- avg rating given by the user
        SELECT ROUND(AVG(rating),2)
        FROM ratings_staging
        WHERE user_id = r.user_id
    ) AS avg_user_rating,        
    
    (           -- total number of ratings given by the user
        SELECT COUNT(*)
        FROM ratings_staging
        WHERE user_id = r.user_id
    ) AS num_of_user_ratings,  
    CASE WHEN r.rating >= 4 THEN 'Yes' ELSE 'No' END AS user_recommends -- recommendation (if user rating >= 4)
FROM ratings_staging r
JOIN dim_dates dd ON CAST(r.rated_at AS DATE) = dd.rated_at
JOIN dim_time dt ON CAST(r.rated_at AS TIME) = dt.time
JOIN dim_users du ON du.dim_userID = r.user_id
JOIN dim_movies dm ON dm.dim_movieID = r.movie_id
LEFT JOIN dim_genres dg ON dg.genre_name = dm.main_genre
LEFT JOIN dim_tags dtg ON dm.first_tag = dtg.tag_name;

-- drop table fact_ratings;
-- select * from fact_ratings order by fact_ratingid;


--                                      Staging tables - DROP
DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;