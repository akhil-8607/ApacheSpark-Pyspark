from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

df = (spark.read.format("csv")
.option("header", "true")
.option("inferSchema","true")
.option("nullValue", "") # Replace any null data field with quotes
.load("Fake_data.csv"))

#SQL 

#1)
print("\n")
df.createOrReplaceTempView("people")
spark.sql("SELECT Birth_Country, COUNT(Birth_Country) FROM people  GROUP BY birth_country HAVING COUNT (birth_country)=(SELECT MAX(ct) FROM ( SELECT birth_country, COUNT(birth_country) ct FROM people GROUP BY birth_country))").show()

#2)
print("\n")
spark.sql('with usa_ppl as (select * from people where birth_country="United States of America") SELECT distinct birth_country,avg(income) from usa_ppl group by birth_country').show()

#3)
print("\n")
spark.sql('with  p as (select * from people WHERE Loan_Approved="false" AND Income > 100000) select count(first_name) as people_filtred from p').show()

#4)
print("\n")
spark.sql('with p as (select  first_name,last_name,income,job from people  where Birth_Country="United States of America") select * from p order by income desc limit 10').show()

#5)
print("\n")
spark.sql('with dist_jobs as (select distinct job FROM people) select count(*) as distinct_jobs from dist_jobs').show()

#6)
print("\n")
spark.sql('with ppl_100000 as (select * from people WHERE Job="Writer" AND Income<100000) select count(first_name) as Writers_num from ppl_100000').show()