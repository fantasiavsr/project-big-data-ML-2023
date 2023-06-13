from pyspark import *
sc = SparkContext.getOrCreate();
broadcastVar = sc.broadcast(list(range(1, 100)))
# broadcastVar.value 	
print(broadcastVar.value)