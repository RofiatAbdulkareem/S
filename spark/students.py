from pyspark.sql import SparkSession #import library

spark = SparkSession.builder\
    .appName("Student Scores Project")\
    .getOrCreate()

df = spark.read.csv("students.csv", header=True, inferSchema=True) #loading csv

df_clean = df.dropna("Country")
avg_maths = df_clean 