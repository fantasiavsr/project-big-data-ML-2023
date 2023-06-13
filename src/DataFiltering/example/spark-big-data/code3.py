from pyspark import *
# sc = SparkContext()
sc = SparkContext.getOrCreate();

# Get the lines from the textfile, create 4 partitions
access_log = sc.textFile("code/practicum_week7/data/README.md", 4)

#Filter Lines with ERROR only
error_log = access_log.filter(lambda x: "Spark" in x)

# Cache error log in memory
cached_log = error_log.cache()

# Now perform an action -  count
print ("Total number of Spark records are %s" % (cached_log.count()))

# Now find the number of lines with 
print ("Number of product pages visited that have Spark is %s" % (cached_log.filter(lambda x: "product" in x).count()))