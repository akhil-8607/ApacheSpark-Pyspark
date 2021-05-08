import json
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext


spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

path_json = "/opt/spark/examples/src/main/resources/people.json"
path_csv = "/opt/spark/examples/src/main/resources/people.csv"

# Question :-1

print("Question-1")
print("\n")

df = spark.read.format("json").load(path_json)
print(df.show())

# Question :-2
print("\n")
print("Question-2")

hive = HiveContext(sc)
people = hive.read.json(path_json)
people.printSchema()
people.registerTempTable("data")
names = hive.sql("SELECT DISTINCT name FROM data")
ages= hive.sql("SELECT DISTINCT age FROM data")
print("\n")
for i in names.collect():
	print(i.name)
print("\n")
for row in ages.collect():
	print(row.age)


# Question:-3 
print("\n")
print("Question-3")

df = (spark.read.format("csv")
.option("header", "true")
.option("inferSchema","true")
.option("mode", "FAILFAST") # Exit if any errors
.option("nullValue", "") # Replace any null data field with quotes
.load(path_csv))
print("\n")
print(df.show())
print("\n")
print(df.printSchema())
print("\n")
print(df.rdd)

