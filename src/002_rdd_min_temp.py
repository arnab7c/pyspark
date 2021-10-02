# configure pyspark with spark context
from pyspark import SparkConf, SparkContext

conf = SparkConf ().setMaster ( "local" ).setAppName ( "MinTemperaturs" )
sc = SparkContext ( conf=conf )


def parseLine(line):
    fields = line.split ( "," )
    stationId = fields[ 0 ]
    entryType = fields[ 2 ]
    temperature = float ( fields[ 3 ] ) * 0.1 * (9.0 / 5.0) + 32.0
    return stationId, entryType, temperature


# path setup for HDFS file
path = "/user/itv000480/dataset/ds1/"
files = path + "1800.csv"

# read HDFS file line by line
lines = sc.textFile ( files )
# apply parseLine function to each line of lines
parsedLine = lines.map ( parseLine )
# filter only records where entryType is 'TMIN' - (filter by expression)
minTemps = parsedLine.filter ( lambda x: 'TMIN' in x[ 1 ] )
stationTemps = minTemps.map ( lambda x: (x[ 0 ], x[ 2 ]) )
# group records based on key x and return min value of y - (rollup)
minTemps = stationTemps.reduceByKey ( lambda x, y: min ( x, y ) )

results = minTemps.collect ()

for result in results:
    print ( result[ 0 ] + "\t{:.2f}F".format ( result[ 1 ] ) )
