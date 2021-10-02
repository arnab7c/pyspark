# configure pyspark with spark context
from pyspark import SparkConf, SparkContext

conf = SparkConf ().setMaster ( "local" ).setAppName ( "FriendsByAge" )
sc = SparkContext ( conf=conf )


def parseLine(line):
    fields = line.split ( "," )
    age = int ( fields[ 2 ] )
    numFriends = int ( fields[ 3 ] )
    return (age, numFriends)


# path for spark file
path = "/user/itv000480/dataset/ds1/"
files = path + "fakefriends.csv"

# read text file line by line (Input File)
lines = sc.textFile ( files )

# map function will apply parseline function to each element of RDD lines (Reformat)
rdd = lines.map ( parseLine )

# mapValues will only work on value unlike map which works on both key and value
# reduceByKey works on key value pair and merge on key
totalsByAge = rdd.mapValues ( lambda x: (x, 1) ).reduceByKey ( lambda x, y: (x[ 0 ] + y[ 0 ], x[ 1 ] + y[ 1 ])
                                                               )
averagesByAge = totalsByAge.mapValues ( lambda x: x[ 0 ] / x[ 1 ] )
results = averagesByAge.collect ()

for result in results:
    print ( result )
