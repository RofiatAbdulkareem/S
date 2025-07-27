from pyspark.sql import SparkSession #import library

spark = SparkSession.builder\
    .appName("Age Filter Project")\
    .getOrCreate()  #initializing spark session

df = spark.read.csv("people.csv", header=True, inferSchema=True) #loading csv

df_filtered = df.filter(df.AGE > 18) # filter ages greater than 18

df_grouped = df.groupBy("COUNTRY").count()

df_filtered.write.csv("output/filtered_people", header = True)