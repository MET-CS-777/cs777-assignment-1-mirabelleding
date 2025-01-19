from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

#Main
if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="Assignment-1")
    
    rdd = sc.textFile(sys.argv[1])
    spark = SparkSession.builder.appName("Assignment-1").getOrCreate()

    # Clean data
    data = rdd.map(lambda line: line.split(",")) \
              .map(correctRows) \
              .filter(lambda x: x is not None)  # Remove invalid rows

    columns = [
        "medallion", "hack_license", "pickup_datetime", "dropoff_datetime",
        "trip_time_in_secs", "trip_distance", "pickup_longitude", "pickup_latitude",
        "dropoff_longitude", "dropoff_latitude", "payment_type", "fare_amount",
        "surcharge", "mta_tax", "tip_amount", "tolls_amount", "total_amount"
    ]
    df = data.toDF(columns)
    
   # Task 1: Top 10 active taxis
    result1 = df.groupBy("medallion").agg(
        countDistinct("hack_license").alias("driver_count")
    ).orderBy("driver_count", ascending=False).limit(10)
    
    result1.coalesce(1).write.csv(sys.argv[2], header=True)

    # Task 2: Top 10 best drivers
    earnings_per_minute = df.withColumn(
        "earnings_per_minute",
        col("total_amount") / (col("trip_time_in_secs") / 60)
    )
    result2 = earnings_per_minute.groupBy("hack_license").agg(
        avg("earnings_per_minute").alias("avg_earnings_per_minute")
    ).orderBy("avg_earnings_per_minute", ascending=False).limit(10)
    
    result2.coalesce(1).write.csv(sys.argv[3], header=True)

    # Task 3: Best hour for profit ratio
    df = df.withColumn("pickup_hour", hour(col("pickup_datetime")))
    df = df.withColumn("profit_ratio", col("surcharge") / col("trip_distance"))
    result3 = df.groupBy("pickup_hour").agg(
        mean("profit_ratio").alias("avg_profit_ratio")
    ).orderBy("avg_profit_ratio", ascending=False).limit(1)
    
    result3.coalesce(1).write.csv(sys.argv[4], header=True)
    
    #Task 4 - Optional 
    #Your code goes here


    sc.stop()
