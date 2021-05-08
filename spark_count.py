from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("assigment-2")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
text_file = sc.textFile("assignment_2_datafile.txt")
counts = text_file.flatMap(lambda line: line.split(" ")).filter(lambda x: len(x)>3).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
print('counts:-',counts)
counts.saveAsTextFile("Akhil/")

