from pyspark.sql.functions import avg, trim, regexp_extract, explode, split, col, lower, concat_ws, collect_list
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType

from Utililty import get_raw_data_directory


spark = (
    SparkSession.builder
    .appName("Movies")
    .master("local")
    .getOrCreate()
)

# Ab Initio - Input File
# read movie.csv and create dataframe
movie_csv = get_raw_data_directory() + "/movie/movie.csv"
df_movie = spark.read.csv(movie_csv, header=True, inferSchema=True)

# read rating.csv and create dataframe
rating_csv = get_raw_data_directory() + "/movie/rating.csv"
df_rating = spark.read.csv(rating_csv, header=True, inferSchema=True)
df_rating.createOrReplaceTempView("rating")

# read tag.csv and create dataframe
tag_csv = get_raw_data_directory() + "/movie/tag.csv"
df_tag = spark.read.csv(tag_csv, header=True, inferSchema=True)
df_tag.createOrReplaceTempView("tag")

# Ab Initio - Reformat & Normalize
# from movie.csv create new data frame with movieid, title, year, genre
# ( genres in "|" separated, which should be in different row ) - explode

# Split into Title and Year and genre
df_movie_cleaned = df_movie.withColumn(
    "year",
    regexp_extract("title", r"\((\d{4})\)$", 1).cast("int")
).withColumn(
    "title",
    trim(regexp_extract("title", r"^(.*)\s\(\d{4}\)$", 1))
).withColumn(
    "genre",
    explode(split(col("genres"), r"\|"))
)

df_movie_cleaned.createOrReplaceTempView("movie")

# Create one row for each unique genre.
df_genre = df_movie_cleaned.select("genre").distinct().orderBy("genre")

# Join df_movie_cleaned with rating.csv to find top 10 movie for each genre based on average
# rating for year selected
df_filtered_movie = (
    df_movie_cleaned
    .select("movieId","title", "year", "genre")
    .filter( (df_movie_cleaned.year == 1992) & (lower(df_movie_cleaned.genre) == "thriller"))
    )

# Average rating of the movie
df_avg_rating = (
    df_rating
    .groupBy("movieId")
    .agg(avg("rating").alias("avg_rating"))
    .withColumn(
        "avg_rating",col("avg_rating").cast(DecimalType(10,2)) )
)

# Set up tag list for the movie
df_tag_list = (
    df_tag.groupBy("movieId").agg(
        concat_ws(",",collect_list("tag").alias("tags")).alias("tag_list")
    )
)

df_movie_set = (
    df_filtered_movie
    .join(df_avg_rating, on="movieId", how="inner")
    .join(df_tag_list, on="movieId", how="inner")
    .orderBy(col("avg_rating").desc())
)

df_movie_set.show(20)
