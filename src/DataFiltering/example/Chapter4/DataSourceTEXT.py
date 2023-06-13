# To load text files, we use �text� method which will return a single column with column name as �value� and type as string.
from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("CreatingDataFrames").getOrCreate()
df_txt = spark.read.text("data/Chapter4/examples/src/main/resources/people.txt")
df_txt.show()
df_txt
# DataFrame[value: string]
