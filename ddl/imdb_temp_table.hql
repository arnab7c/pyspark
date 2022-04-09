CREATE DATABASE IF NOT EXISTS itv000480_imdb LOCATION '/user/itv000480/database/imdb.db';
use itv000480_imdb;

-- create temp table & load name.basics data

CREATE TEMPORARY TABLE temp_name_basics (
	nconst STRING COMMENT 'Surrogate Key',
	primary_name STRING COMMENT 'Name of the person',
	birth_year INT COMMENT 'birth year of the person',
	death_year INT COMMENT 'Death year of the person',
	primary_profession STRING COMMENT 'Primary profession of the person',
	known_for_titles STRING COMMENT 'Surrogate key of important work done by the person'
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="1")
;

LOAD DATA LOCAL INPATH '/data/imdb/name.basics.tsv' INTO TABLE temp_name_basics;

insert overwrite table name_basics select * from temp_name_basics;

-- create temp table & load title.crew data

CREATE TEMPORARY TABLE temp_title_crew (
	tconst STRING COMMENT 'Title Surrogate Key',
	directors STRING COMMENT 'Name Surrogate Key',
	writers STRING COMMENT 'Name of the writers'
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="1")
;

LOAD DATA LOCAL INPATH '/data/imdb/title.crew.tsv' INTO TABLE temp_title_crew;

insert overwrite table title_crew select * from temp_title_crew;

-- create temp table & load title.crew data

CREATE TEMPORARY TABLE temp_title_episode (
	tconst STRING COMMENT 'Episode Title Surrogate Key',
	parent_tconst STRING COMMENT 'Episode Surrogate Key',
	season_number INT COMMENT 'Season Number',
	episode_number INT COMMENT 'Episode Number'
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="1")
;

LOAD DATA LOCAL INPATH '/data/imdb/title.episode.tsv' INTO TABLE temp_title_episode;

insert overwrite table title_episode select * from temp_title_episode;

-- create temp table & load title.principals data

CREATE TEMPORARY TABLE temp_title_principal (
	tconst STRING COMMENT 'Episode Title Surrogate Key',
	ordering INT ,
	nconst STRING COMMENT 'Surrogate Key',
	category STRING COMMENT 'category of the title',
	job STRING COMMENT 'Name of the tile',
	characters STRING COMMENT 'character of the person in title'
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="1")
;

LOAD DATA LOCAL INPATH '/data/imdb/title.principals.tsv' INTO TABLE temp_title_principal;

insert overwrite table title_principal select * from temp_title_principal;

-- create temp table & load title.ratings data

CREATE TEMPORARY TABLE temp_title_ratings (
	tconst STRING COMMENT 'Episode Title Surrogate Key',
	average_rating STRING COMMENT 'Average Rating of the title',
	num_votes INT COMMENT 'Number of votes'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="1")
;

LOAD DATA LOCAL INPATH '/data/imdb/title.ratings.tsv' INTO TABLE temp_title_ratings;

insert overwrite table title_ratings select * from temp_title_ratings;

-- create temp table & load title.basics data

CREATE TEMPORARY TABLE temp_title_basics (
	tconst STRING COMMENT 'Episode Title Surrogate Key',
	title_type STRING COMMENT 'Title length type',
	primary_title STRING COMMENT 'Main title',
	original_title STRING COMMENT 'Original title',
	is_adult INT COMMENT 'Is the title adult?',
	start_year INT COMMENT 'Start year of the title',
	end_year INT COMMENT 'End year of the title',
	run_time_min INT COMMENT 'Number of min title ran',
	genres STRING COMMENT 'Genre of the title'
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="1")
;

LOAD DATA LOCAL INPATH '/data/imdb/title.basics.tsv' INTO TABLE temp_title_basics;

insert overwrite table title_basics select * from temp_title_basics;

-- create temp table & load title.akas data

CREATE TEMPORARY TABLE temp_title_akas (
	title_id STRING COMMENT 'Episode Title Surrogate Key',
	ordering INT,
	title STRING COMMENT 'Title of the episode',
	region STRING COMMENT 'Region of the content',
	language STRING COMMENT 'language of the content',
	types STRING,
	attributes STRING,
	is_original_title STRING
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="1")
;

LOAD DATA LOCAL INPATH '/data/imdb/title.akas.tsv' INTO TABLE temp_title_akas;

insert overwrite table title_akas select * from temp_title_akas;

