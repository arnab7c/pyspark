-- create database for imdb project
CREATE DATABASE IF NOT EXISTS itv000480_imdb LOCATION '/user/itv000480/database/imdb.db';
use itv000480_imdb;

-- create imdb tables
CREATE TABLE IF NOT EXISTS name_basics (
nconst STRING COMMENT 'Surrogate Key',
primary_name STRING COMMENT 'Name of the person',
birth_year INT COMMENT 'birth year of the person',
death_year INT COMMENT 'Death year of the person',
primary_profession STRING COMMENT 'Primary profession of the person',
known_for_titles STRING COMMENT 'Surrogate key of important work done by the person'
) STORED AS parquet
;

CREATE TABLE IF NOT EXISTS title_crew (
tconst STRING COMMENT 'Title Surrogate Key',
directors STRING COMMENT 'Name Surrogate Key',
writers STRING COMMENT 'Name of the writers')
STORED AS parquet
;

CREATE TABLE IF NOT EXISTS title_episode (
tconst STRING COMMENT 'Episode Title Surrogate Key',
parent_tconst STRING COMMENT 'Episode Surrogate Key',
season_number INT COMMENT 'Season Number',
episode_number INT COMMENT 'Episode Number'
) STORED AS parquet
;

CREATE TABLE IF NOT EXISTS title_principal (
tconst STRING COMMENT 'Episode Title Surrogate Key',
ordering INT ,
nconst STRING COMMENT 'Surrogate Key',
category STRING COMMENT 'category of the title',
job STRING COMMENT 'Name of the tile',
characters STRING COMMENT 'character of the person in title'
) STORED AS parquet
;

CREATE TABLE IF NOT EXISTS title_ratings (
tconst STRING COMMENT 'Episode Title Surrogate Key',
average_rating STRING COMMENT 'Average Rating of the title',
num_votes INT COMMENT 'Number of votes'
) STORED AS parquet
;


CREATE TABLE IF NOT EXISTS title_basics (
tconst STRING COMMENT 'Episode Title Surrogate Key',
title_type STRING COMMENT 'Title length type',
primary_title STRING COMMENT 'Main title',
original_title STRING COMMENT 'Original title',
is_adult INT COMMENT 'Is the title adult?',
start_year INT COMMENT 'Start year of the title',
end_year INT COMMENT 'End year of the title',
run_time_min INT COMMENT 'Number of min title ran',
genres STRING COMMENT 'Genre of the title'
) STORED AS parquet
;

CREATE TABLE IF NOT EXISTS title_akas (
title_id STRING COMMENT 'Episode Title Surrogate Key',
ordering INT,
title STRING COMMENT 'Title of the episode',
region STRING COMMENT 'Region of the content',
language STRING COMMENT 'language of the content',
types STRING,
attributes STRING,
is_original_title STRING
) STORED AS parquet
;
