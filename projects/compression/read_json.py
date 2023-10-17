import pandas as pd
import pyspark
from pyspark.sql import SparkSession
import os
import sqlite3

filename = "questions"
json_path = os.path.join(os.getcwd(), "data", filename + ".json")

spark = SparkSession.builder.getOrCreate()
df = spark.read.json(json_path)

print(df.printSchema())
df_pd = df.toPandas()

con = sqlite3.connect(os.path.join(os.getcwd(), "data", filename + ".db"))
df_pd.to_sql(filename, con, index=True, if_exists="replace")
