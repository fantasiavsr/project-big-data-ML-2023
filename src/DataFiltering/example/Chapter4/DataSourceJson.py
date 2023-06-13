# Copy input dataset(people.json) to HDFS and then get into PySpark Shell. To read json file, use format to specify the type of datasource. 
from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("CreatingDataFrames").getOrCreate()
df_json = spark.read.load("data/Chapter4/examples/src/main/resources/people.json", format="json")
df_json = spark.read.json("data/Chapter4/examples/src/main/resources/people.json")
df_json.printSchema()

df_json.show()

# To write data to another JSON file, use below command.

df_json.write.json("newjson_dir")
df_json.write.format("json").save("newjson_dir2")

# To write data to any other format, just mention format you want to save. Below example saves df_json DataFrame in Parquet format. 

df_json.write.parquet("parquet_dir")
df_json.write.format("parquet").save("parquet_dir2")
