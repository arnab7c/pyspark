from pyspark.sql import SparkSession
from pyspark.sql import Row

# create a spark session
spark = SparkSession.builder.appName ( "SparkSql" ).getOrCreate ()


# split file with datatype
def mapper(line):
    fields = line.split ( "," )
    return Row ( ID=int ( fields[ 0 ] ), name=str ( fields[ 1 ].encode ( "utf-8" ) ),
                 age=int ( fields[ 2 ] ), numFriends=int ( fields[ 3 ] ) )


# path for spark file
path = "/user/itv000480/dataset/ds1/"
files = path + "fakefriends.csv"

lines = spark.sparkContext.textFile ( files )
# convert the data in dataframe (structured data with row object)
people = lines.map ( mapper )
# cache() will keep the data in memory for different query
schemaPeople = spark.createDataFrame ( people ).cache ()
# people is temp view which is like people database
schemaPeople.createOrReplaceTempView ( "people" )
# return new dataframe teenagers from the query
teenagers = spark.sql ( "SELECT * FROM people WHERE age >= 13 and age <= 19" )

for teen in teenagers.collect ():
    print ( teen )

# can be use below format instead of sql
schemaPeople.groupBy ( "age" ).count ().orderBy ( "age" ).show ()

schemaPeople.groupBy ( "age" ).avg("numFriends").show()
# stop the spark session
spark.stop ()
