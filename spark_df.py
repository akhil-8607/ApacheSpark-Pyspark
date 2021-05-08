from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# Spark DataFrame 

#0) Load fake_data.csv into spark dataframe

df = (spark.read.format("csv")
.option("header", "true")
.option("inferSchema","true")
.option("nullValue", "") # Replace any null data field with quotes
.load("Fake_data.csv"))

# Print Data frame and Schema 

print("\n")
print(df.show())
print("\n")
print(df.printSchema())
print("\n")


#1)
print("\n")
count_df=df.groupby('Birth_Country').agg({'Birth_Country':'count'})
max_count=count_df.orderBy('count(Birth_Country)', ascending=False)
max_count.show(1)

#2)
print("\n")
us=df.filter(df.Birth_Country=='United States of America')
df_avg=us.groupby('Birth_Country').agg({'Income':'mean'})
df_avg.show()

#3) 
print("\n")
loan=df.filter(df.Loan_Approved=='false')
income=loan.filter(loan.Income >100000)
income_count=income.count()
print(income_count)

#4)
print("\n")
filtered=df.filter(df.Birth_Country=='United States of America')
ordered=filtered.select('First_name', 'Last_name', 'Income','Job').orderBy('Income',ascending=False)
ordered.show(10)

#5) 
print("\n")
dist=df.select('Job').distinct()
print(dist.count())

#6)
print("\n")
writers=df.filter(df.Job=='Writer')
num=writers.filter(df.Income<100000)
print(num.count())
num.show()


