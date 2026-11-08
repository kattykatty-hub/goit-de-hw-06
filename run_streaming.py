import argparse
from pathlib import Path
from pyspark.sql import SparkSession,functions as F,types as T
from src.config import INPUT_TOPIC,OUTPUT_TOPIC,kafka_options

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--checkpoint",default="checkpoints/alerts")
    parser.add_argument("--starting-offsets",choices=["earliest","latest"],default="latest")
    args=parser.parse_args()
    spark=SparkSession.builder.appName("GoIT-HW06-Spark-Alerts").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone","UTC")
    schema=T.StructType([T.StructField("id",T.IntegerType()),T.StructField("temperature",T.DoubleType()),T.StructField("humidity",T.DoubleType()),T.StructField("timestamp",T.StringType())])
    raw=(spark.readStream.format("kafka").options(**kafka_options()).option("subscribe",INPUT_TOPIC).option("startingOffsets",args.starting_offsets).load())
    readings=(raw.select(F.from_json(F.col("value").cast("string"),schema).alias("sensor"))
              .select("sensor.*")
              .withColumn("event_time",F.to_timestamp("timestamp"))
              .filter("id IS NOT NULL AND temperature IS NOT NULL AND humidity IS NOT NULL AND event_time IS NOT NULL"))
    averages=(readings.withWatermark("event_time","10 seconds")
              .groupBy(F.window("event_time","1 minute","30 seconds"))
              .agg(F.avg("temperature").alias("t_avg"),F.avg("humidity").alias("h_avg")))
    rule_schema=T.StructType([T.StructField("id",T.IntegerType()),T.StructField("humidity_min",T.DoubleType()),T.StructField("humidity_max",T.DoubleType()),T.StructField("temperature_min",T.DoubleType()),T.StructField("temperature_max",T.DoubleType()),T.StructField("code",T.IntegerType()),T.StructField("message",T.StringType())])
    rules=(spark.read.option("header",True).schema(rule_schema).csv(str(Path(__file__).resolve().parent/"data"/"alerts_conditions.csv")))
    rules.show(truncate=False)
    matched=(averages.crossJoin(F.broadcast(rules)).where(
        ((F.col("humidity_min")==-999)&(F.col("humidity_max")==-999) | F.col("h_avg").between(F.col("humidity_min"),F.col("humidity_max"))) &
        ((F.col("temperature_min")==-999)&(F.col("temperature_max")==-999) | F.col("t_avg").between(F.col("temperature_min"),F.col("temperature_max")))
    ))
    alerts=matched.select(F.struct(F.date_format("window.start","yyyy-MM-dd'T'HH:mm:ss'Z'").alias("start"),F.date_format("window.end","yyyy-MM-dd'T'HH:mm:ss'Z'").alias("end")).alias("window"),F.round("t_avg",2).alias("t_avg"),F.round("h_avg",2).alias("h_avg"),"code","message",F.date_format(F.current_timestamp(),"yyyy-MM-dd'T'HH:mm:ss'Z'").alias("timestamp"))
    output=alerts.select(F.col("code").cast("string").alias("key"),F.to_json(F.struct(*alerts.columns)).alias("value"))
    query=(output.writeStream.format("kafka").options(**kafka_options()).option("topic",OUTPUT_TOPIC)
           .option("checkpointLocation",str(Path(args.checkpoint).resolve())).outputMode("append").start())
    print(f"STREAMING {INPUT_TOPIC} -> {OUTPUT_TOPIC}; waiting for watermark to close windows",flush=True)
    try: query.awaitTermination()
    finally: spark.stop()
if __name__=="__main__":main()
