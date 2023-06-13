from pyspark import *
sc = SparkContext.getOrCreate();
mylist = ["my", "pair", "rdd"]
myRDD = sc.parallelize(mylist)
myPairRDD = myRDD.map(lambda s: (s, len(s)))
# myPairRDD.collect()
print(myPairRDD.collect())
#[('my', 2), ('pair', 4), ('rdd', 3)]

# myPairRDD.keys().collect()
print(myPairRDD.keys().collect())
#['my', 'pair', 'rdd']

# myPairRDD.values().collect()
print(myPairRDD.values().collect())
#[2, 4, 3]
