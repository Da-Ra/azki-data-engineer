from __future__ import annotations

import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    concat_ws,
    count,
    from_json,
    lit,
    sha1,
    sum as _sum,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(
    format="%(asctime)s %(levelname)s streaming-etl | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC      = os.environ.get("KAFKA_TOPIC", "user_events")
STARTING_OFFSETS = os.environ.get("KAFKA_STARTING_OFFSETS", "earliest")
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", 5000))

MYSQL_URL      = os.environ.get("MYSQL_JDBC_URL", "jdbc:mysql://mysql:3306/azki?useSSL=false&allowPublicKeyRetrieval=true")
MYSQL_USER     = os.environ.get("MYSQL_USER", "azki_reader")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "azki_reader_pw")
MYSQL_DRIVER   = "com.mysql.cj.jdbc.Driver"

CH_URL      = os.environ.get("CH_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/azki")
CH_USER     = os.environ.get("CH_USER", "default")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "")
CH_DRIVER   = "com.clickhouse.jdbc.ClickHouseDriver"

CHECKPOINT_BASE  = os.environ.get("CHECKPOINT_BASE", "/checkpoints")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "10 seconds")
WATERMARK        = os.environ.get("WATERMARK", "1 hour")
WINDOW_DURATION  = os.environ.get("WINDOW_DURATION", "1 hour")

EVENT_SCHEMA = StructType([
    StructField("event_time",     StringType(),  False),
    StructField("user_id",        IntegerType(), False),
    StructField("session_id",     StringType(),  False),
    StructField("event_type",     StringType(),  False),
    StructField("channel",        StringType(),  False),
    StructField("premium_amount", LongType(),    False),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("user-events-streaming-etl")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def load_users_broadcast(spark: SparkSession) -> DataFrame:
    log.info("loading users from MySQL")
    df = (
        spark.read.format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", "users")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", MYSQL_DRIVER)
        .option("fetchsize", "10000")
        .load()
        .select(
            col("user_id").cast("int").alias("user_id"),
            col("signup_date").cast("date").alias("signup_date"),
            col("city"),
            col("device_type"),
        )
    )
    n = df.count()
    log.info(f"  loaded {n:,} users (will be broadcast-joined)")
    return broadcast(df)


def parse_kafka_stream(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )
    return (
        raw.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"))
           .select("e.*")
           .withColumn("event_time", to_timestamp(col("event_time")))
           .withColumn(
               "event_hash",
               sha1(concat_ws(
                   "|",
                   col("event_time").cast("string"),
                   col("user_id").cast("string"),
                   col("session_id"),
                   col("event_type"),
               )),
           )
    )


def write_jdbc(df: DataFrame, table: str, batch_id: int) -> None:
    df.persist()
    try:
        n = df.count()
        if n == 0:
            return
        log.info(f"  batch {batch_id}: writing {n:,} rows → clickhouse.{table}")
        (
            df.write.format("jdbc")
              .mode("append")
              .option("url", CH_URL)
              .option("dbtable", table)
              .option("user", CH_USER)
              .option("password", CH_PASSWORD)
              .option("driver", CH_DRIVER)
              .option("batchsize", "5000")
              .option("isolationLevel", "NONE")
              .save()
        )
    finally:
        df.unpersist()


def main() -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    log.info("=== streaming-etl starting ===")
    log.info(f"  kafka={KAFKA_BOOTSTRAP}/{KAFKA_TOPIC}  ch={CH_URL}  mysql={MYSQL_URL}")

    users_bc = load_users_broadcast(spark)
    events   = parse_kafka_stream(spark)

    enriched = (
        events.join(users_bc, on="user_id", how="left")
              .select(
                  "event_time", "user_id", "session_id", "event_type", "channel",
                  "premium_amount", "city", "device_type", "signup_date", "event_hash",
              )
    )

    raw_query = (
        enriched.writeStream
        .queryName("raw_to_clickhouse")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/raw")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .foreachBatch(lambda df, bid: write_jdbc(df, "raw_user_events", bid))
        .start()
    )

    rollup = (
        events.join(users_bc, on="user_id", how="left")
              .filter(col("event_type") == "purchase")
              .withWatermark("event_time", WATERMARK)
              .groupBy(
                  window(col("event_time"), WINDOW_DURATION).alias("w"),
                  col("channel"),
                  col("city"),
              )
              .agg(
                  count(lit(1)).cast("int").alias("purchase_count"),
                  _sum("premium_amount").cast("long").alias("total_premium"),
                  avg("premium_amount").alias("avg_premium"),
              )
              .select(
                  col("w.start").alias("window_start"),
                  "channel",
                  "city",
                  "purchase_count",
                  "total_premium",
                  col("avg_premium").cast("double").alias("avg_premium"),
              )
    )

    agg_query = (
        rollup.writeStream
        .queryName("agg_to_clickhouse")
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/agg")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .foreachBatch(lambda df, bid: write_jdbc(df, "revenue_rollup_hourly", bid))
        .start()
    )

    log.info("streaming queries started — awaiting termination")
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
