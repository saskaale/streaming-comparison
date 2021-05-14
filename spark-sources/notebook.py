from pyspark import RDD
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import pandas as pd
from pyspark import RDD
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, col, desc, concat, lit, count

from pyspark.sql.window import Window

# Import SQLContext and data types
from pyspark.sql import SQLContext
from pyspark.sql.types import *

sc = spark.sparkContext
ssc = StreamingContext(sc,2)

# sc is an existing SparkContext.
sqlContext = SQLContext(sc)


def printTo(stream):
  stream \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()


df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "40.89.150.165:9092") \
  .option("subscribe", "quickstart-events") \
  .load()


inpData = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

inpData = inpData.withWatermark("timestamp", "20 seconds")
inpData = inpData.withColumn('timestampGMT', inpData.timestamp.cast('timestamp'))

w = window("timestamp", "10 second", "5 seconds")


def writeToTopic(topic, data, mode="update"):
  from pyspark.sql.functions import to_json, col, struct
  
  print("data")
  print(data)
  
  (data \
      .writeStream \
      .outputMode(mode) \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "40.89.150.165:9092") \
      .option("checkpointLocation", f"/FileStore/{topic}-checkpoint15") \
      .option("topic", topic) \
      .start() \
  )

  pass




total = inpData.groupBy(w).agg(
     count(lit(1)).alias("count")
).orderBy(desc("window")).limit(1)



writeToTopic(
  "out-total", 
  total.selectExpr("CAST(count AS STRING) as value"),
  mode="complete"
)


grep = inpData \
  .transform(lambda rdd: rdd.filter(col("value") == lit("dolore"))) \
  .groupBy(w).agg(
     count(lit(1)).alias("count")
).orderBy(desc("window")).limit(1)

writeToTopic(
  "out-grep", 
  grep.selectExpr("CAST(count AS STRING) as value"),
  mode="complete"
)

  
rollingGroups = inpData \
  .groupBy("value", w) \
  .agg( \
       count(lit(1)).alias("count") \
  )
rollingGroups \
  .createOrReplaceTempView("groups")

topK = sqlContext.sql("SELECT * FROM groups ORDER BY window DESC, count DESC LIMIT 10")

# def trarnsformTopK(rdd):
#   print(rdd)
#   return rdd
# topK = topK.transform(trarnsformTopK)


writeToTopic(
  "out-topk", 
  topK.selectExpr("CONCAT(value, ' - ', CAST(count as string)) as value"),
  mode="complete"
)


