#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import os
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types


spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()


print("SPARKSESSION SET")



#os.system('wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz')


#pf = 'fhvhv_tripdata_2021-01.parquet'
#df = pd.read_parquet(pf)
#df.to_csv(csv, index=False, sep=',')


csvgz = 'fhvhv_tripdata_2021-06.csv.gz'
xcsv = 'fhvhv_tripdata_2021-06.csv'
csv = 'tripdata.csv'

#os.system(f"gunzip -f {csvgz}")


df = spark.read \
    .option("header", "true") \
    .csv(xcsv)


f = pd.read_csv(xcsv, usecols=[0,1,2,3,4])
f.to_csv(csv, index=False)


os.system(f"wc -l {csv}")


df = spark.read \
    .option("header", "true") \
    .csv(csv)


df.head(5)


df.schema


os.system(f"head -n 101 {csv} > head.csv")


os.system('head -n 10 head.csv')


print("START WORD COUNT OF head.csv")


os.system('wc -l head.csv')


df_pandas = pd.read_csv('head.csv')


df_pandas.dtypes


spark.createDataFrame(df_pandas).show()


spark.createDataFrame(df_pandas).schema


print("SPARK.CREATEDATAFRAME(DF_PANDAS).SCHEMA")


schema = types.StructType([
        types.StructField('dispatching_base_num', types.StringType(), True),
        types.StructField('pickup_datetime', types.TimestampType(), True),
        types.StructField('dropoff_datetime', types.TimestampType(), True),
        types.StructField('PULocationID', types.IntegerType(), True),
        types.StructField('DOLocationID', types.IntegerType(), True)
    ])


print("SCHEMA TYPES CREATED ---")


df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(csv)


df.show()


df.head(10)


df = df.repartition(12)


df.write.parquet('fhvhv/2021/06/', mode='overwrite')


df = spark.read.parquet('fhvhv/2021/06/')


df


df.printSchema()


#  df.select('pickup_datetime', 'dropoff_datetime', 'PuLocationId', 'DOLocationID') \
#      .filter(df.hvfhs_license_num == 'HV0003') \
#      .show()
#  
#  
#  # In[70]:
#  
#  
#  df \
#      .withColumn('pickup_datetime', F.to_date(df.pickup_datetime)) \
#      .withColumn('dropoff_datetime', F.to_date(df.dropoff_datetime)) \
#      .show()
#  
#  
#  # In[72]:
#  
#  
#  df \
#      .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
#      .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
#      .show()
#  
#  
#  # In[73]:
#  
#  
#  df \
#      .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
#      .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
#      .select('pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
#      .show()
#  
#  
#  # In[79]:
#  
#  
#  def crazy_stuff(base_num):
#      num = int(base_num[1:])
#      if num % 7 == 0:
#          return f's/{num:03x}'
#      elif num % 3 == 0:
#          return f'a/{num:03x}'
#      else:
#          return f'e{num:03x}'
#  
#  
#  # In[80]:
#  
#  
#  crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())
#  
#  
#  # In[81]:
#  
#  
#  crazy_stuff('B02884')
#  
#  
#  # In[83]:
#  
#  
#  df \
#      .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
#      .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
#      .withColumn('base_id', crazy_stuff_udf(df.dispatching_base_num)) \
#      .select('base_id', 'pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
#      .show()
