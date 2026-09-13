from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, Row

# Create Spark Session
spark = (
    SparkSession.builder
    .appName("Spark_Dataframe")
    .master("local")
    .getOrCreate()
)

# Create python list
ages_list = [20,25,30,45]
names_list = ["Sachin","Rahul","Lara","Messi"]

# Create Spark Dataframe from List - Single Column Dataframe
#df = spark.createDataFrame(ages_list,'int')
df = spark.createDataFrame(names_list, StringType())
df.show()

#Create multi-column Spark Dataframe using python List
#t_ages_list = [(20,),(30,),(40,)]
#t_df = spark.createDataFrame(t_ages_list)
#t_df.show()
t_century_list = [(100,"Sachin"),(90,"Virat"),(70,"Ponting")]
t_century_df = spark.createDataFrame(t_century_list,'century int,batter string')
t_century_df.show()

# Row - Convert List of list in Dataframe using Row
row = Row(centrury=10, batter="Root")
l_century_list = [[100,"Sachin"],[90,"Virat"],[70,"Ponting"]]
l_century_list.append(row)

#list comprehension
batter_rows = [Row(*batter) for batter in l_century_list]
print(batter_rows)
b_df = spark.createDataFrame(batter_rows,'century int,batter string')
b_df.show()

# Row - Convert List of Dicts in Dataframe using Row
t_batter_dicts = [
    {"position":1,"batter":"Sachin"},
    {"position":2,"batter":"Rahul"},
    {"position":3,"batter":"Sehwag"},
    {"position":4,"batter":"Laxman"},
    {"position":5,"batter":"Sourav"}
]

# Approach 1 - values by position
batter_dicts = [Row(*batter.values()) for batter in t_batter_dicts]
df1 = spark.createDataFrame(batter_dicts,'century int,batter string')
df1.show()

# Approach 2 - values by column name - preferred when working with dictionaries
batter_dicts2 = [Row(**batter) for batter in t_batter_dicts]
df2 = spark.createDataFrame(batter_dicts2, 'century int,batter string')
df2.show()
