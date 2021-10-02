# configure pyspark with spark context
from pyspark import SparkConf, SparkContext

conf = SparkConf ().setMaster ( "local" ).setAppName ( "WordCounts" )
sc = SparkContext ( conf=conf )

# path setup for HDFS file
path = "/user/itv000480/dataset/ds1/"
files = path + "customer-orders.csv"

# read HDFS file line by line
lines = sc.textFile ( files )

def parsePurchase(line):
    field = line.split(",")
    customer = field[0]
    amount = float(field[2])
    return customer,amount

customers = lines.map ( parsePurchase ).reduceByKey(lambda x,y : x + y)
customersSorted = customers.map(lambda (x,y):(y,x)).sortByKey()
# tuple unpacking is not valid in python 3

results = customersSorted.collect()

for result in results:
    print ( result[ 1 ] + "\t{:.2f}F".format ( result[ 0 ] ) )