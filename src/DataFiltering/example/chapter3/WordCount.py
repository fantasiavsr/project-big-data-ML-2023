from operator import add
from pyspark import *
sc = SparkContext.getOrCreate();
lines = sc.textFile("data/chapter3/README.md")
counts = lines.flatMap(lambda x: x.split(' ')) \
              .map(lambda x: (x, 1)) \
              .reduceByKey(add)
output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))
