from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

tx_schema = StructType([
    StructField("tx_id",     StringType()),
    StructField("user_id",   StringType()),
    StructField("amount",    DoubleType()),
    StructField("store",     StringType()),
    StructField("category",  StringType()),
    StructField("timestamp", StringType()),
])

spark = (
    SparkSession.builder
    .appName("Lab4-Anomaly")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "transactions")
    .load()
)

parsed = (
    kafka_raw
    .select(from_json(col("value").cast("string"), tx_schema).alias("tx"))
    .select("tx.*")
    .withColumn("event_time", to_timestamp("timestamp"))
)

anomalies = (
    parsed
    .groupBy(
        window(col("event_time"), "60 seconds"),
        col("user_id")
    )
    .count()
    .filter(col("count") > 3)
)

anomalies = anomalies.dropDuplicates(["user_id", "window"])

alerts_formatted = anomalies.selectExpr(
    "concat('ANOMALIA: user_id=', user_id, "
    "' | liczba transakcji=', count, "
    "' | window_start=', window.start, "
    "' | window_end=', window.end) as message"
)

def print_alerts(df, epoch_id):
    for row in df.collect():
        print(row["message"])

q = (
    alerts_formatted.writeStream
    .outputMode("update")
    .foreachBatch(print_alerts)
    .start()
)

q.awaitTermination()
