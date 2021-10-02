from pyspark.sql import SparkSession

# create a spark session
spark = SparkSession.builder.appName("SparkSql").getOrCreate()

# path for spark file
path = "/user/itv000480/dataset/ds1/"
files = path + "fakefriends-header.csv"

people = spark.read.option("header","true").option("inferSchema","true").csv(files)

print("Here is our infer schema ::")
people.printSchema()

print("Lets display the name column ::")
people.select("name").show()

print("Print anyone over 21 ::")
people.filter(people.age > 21).show()

print("Group by age ::")
people.groupBy("age").count().show()

print("Make everyone 10 years older ::")
people.select(people.name,people.age + 10).show()

spark.stop()