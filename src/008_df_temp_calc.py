from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# create a spark session
spark = SparkSession.builder.appName("TempCalulation").getOrCreate()

schema = StructType([ \
		StructField("stationId",StringType(), True), \
		StructField("date",IntegerType(), True), \
		StructField("measureType",StringType(), True), \
		StructField("temperature",FloatType(),True)])

# path for spark file
path = "/user/itv000480/dataset/ds1/"
files = path + "1800.csv"

df = spark.read.schema(schema).csv(files)
df.printSchema()

# filter out all but TMIN entries
minTemps = df.filter(df.measureType == "TMIN")

# select only stationId and temperature
stationTemps = minTemps.select("stationId","temperature")

# Aggregate to find min temp in every station
minTempsByStation = stationTemps.groupBy("stationId").min("temperature")
minTempsByStation.show()

# Collect, format and print result
results = minTempsByStation.collect()

for result in results:
	print(result[0] + "\t{:.2f}C".format(result[1]))

