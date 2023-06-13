from pyspark import *
# sc = SparkContext()
sc = SparkContext.getOrCreate();

mylist = ["my", "pair", "rdd"]
myRDD = sc.parallelize(mylist)
myPairRDD = myRDD.map(lambda s: (s, len(s)))
# myPairRDD.collect()
# myPairRDD.keys().collect()
# myPairRDD.values().collect()
print (myPairRDD.collect())
print (myPairRDD.keys().collect())
print (myPairRDD.values().collect())