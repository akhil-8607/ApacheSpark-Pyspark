import pyspark
sc = pyspark.SparkContext('local[*]')
sc.setLogLevel("WARN")

big_list = range(1000)
rdd = sc.parallelize(big_list, 3)
odds = rdd.filter(lambda x: x % 2 != 0)
print(odds.take(5))

#References:- http://spark.apache.org/examples.html