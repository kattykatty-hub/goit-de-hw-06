"""Optional separate console stream to demonstrate windowed averages."""
from pyspark.sql import SparkSession,functions as F,types as T
from src.config import INPUT_TOPIC,kafka_options
spark=SparkSession.builder.appName("GoIT-HW06-Average-Inspection").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
schema=T.StructType([T.StructField("id",T.IntegerType()),T.StructField("temperature",T.DoubleType()),T.StructField("humidity",T.DoubleType()),T.StructField("timestamp",T.StringType())])
raw=spark.readStream.format("kafka").options(**kafka_options()).option("subscribe",INPUT_TOPIC).option("startingOffsets","latest").load()
readings=raw.select(F.from_json(F.col("value").cast("string"),schema).alias("v")).select("v.*").withColumn("event_time",F.to_timestamp("timestamp")).dropna(subset=["event_time","temperature","humidity"])
avgs=(readings.withWatermark("event_time","10 seconds").groupBy(F.window("event_time","1 minute","30 seconds")).agg(F.round(F.avg("temperature"),2).alias("t_avg"),F.round(F.avg("humidity"),2).alias("h_avg")))
q=avgs.writeStream.format("console").outputMode("update").option("truncate",False).start()
try:q.awaitTermination()
finally:spark.stop()
