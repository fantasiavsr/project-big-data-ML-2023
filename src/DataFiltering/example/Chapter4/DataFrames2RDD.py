# Get into PySpark Shell and execute below commands. 
# Create DataFrame and convert to RDD  
from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("CreatingDataFrames").getOrCreate()
mylist = [(1, "MuhamadAlifRizki-2041720196"),(3, "Big Data 2023")]
myschema = ['col1', 'col2']
df = spark.createDataFrame(mylist, myschema)

#Convert DF to RDD
df.rdd.collect()

df2rdd = df.rdd
df2rdd.take(2)

print(df2rdd.collect())