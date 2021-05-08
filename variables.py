import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("shared-variables").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# Create broadcast variable 

broadcastVar = sc.broadcast([0, 1, 2, 3])

#print broadcast variable

print(broadcastVar.value)

#Accumulator 

#initializing accumulator value 

accum=spark.sparkContext.accumulator(0)

#creating rdd

rdd=spark.sparkContext.parallelize([1,2,3,4,5])
rdd.foreach(lambda x:accum.add(x))

#final value after acuumulating
print(accum.value)