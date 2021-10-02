import re
# configure pyspark with spark context
from pyspark import SparkConf, SparkContext

conf = SparkConf ().setMaster ( "local" ).setAppName ( "WordCounts" )
sc = SparkContext ( conf=conf )


def normalizeWords(text):
    return re.compile ( r'\W+', re.UNICODE ).split ( text.lower () )


# path setup for HDFS file
path = "/user/itv000480/dataset/ds1/"
files = path + "Book"

# read hdfs file line by line
inputs = sc.textFile ( files )

# flatMap will apply lambda function to each element and eah word will become an element in RDD
# difference between map & flatMap - map give same number of element as input
words = inputs.flatMap ( lambda x: x.split () )
betterWords = inputs.flatMap ( normalizeWords )

wordCounts = betterWords.countByValue ()
# words.map ( lambda x:(x,1) ) -> this will create new RDD with each word and a value 1
# reduceByKey ( lambda x,y : x + y ) -> this will create new RDD by adding +1 value each time a word appear
wordCountsNew = words.map( lambda x:(x,1) ).reduceByKey( lambda x,y: x + y )

# lambda (x,y) : (y,x) will flip the RDD where key become value & value is key
# sortByKey() will then sort the RDD based on the number of times a word appeared
wordCountsSorted = wordCountsNew.map( lambda (x,y) : (y,x) ).sortByKey()
results = wordCountsSorted.collect()

for word, count in wordCounts.items ():
    cleanWord = word.encode ( 'ascii', 'ignore' )
    if cleanWord:
        print ( cleanWord.decode () + " " + str ( count ) )

for result in results:
	count = str(result[0])
	word = result[1].encode('ascii','ignore')
	if word:
		print ( word +"\t\t" + count )