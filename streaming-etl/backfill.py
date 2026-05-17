from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    concat_ws,
    from_json,
    sha1,
    to_timestamp,
)
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(
    format="%(asctime)s %(levelname)s spark-backfill | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

MYSQL_URL      = os.environ.get("MYSQL_JDBC_URL", "jdbc:mysql://mysql:3306/azki?useSSL=false&allowPublicKeyRetrieval=true")
MYSQL_USER     = os.environ.get("MYSQL_USER", "azki_reader")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "azki_reader_pw")
MYSQL_DRIVER   = "com.mysql.cj.jdbc.Driver"

CH_URL      = os.environ.get("CH_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/azki")
CH_USER     = os.environ.get("CH_USER", "default")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "")
CH_DRIVER   = "com.clickhouse.jdbc.ClickHouseDriver"

EVENT_SCHEMA = StructType([
    StructField("event_time",     StringType(),  False),
    StructField("user_id",        IntegerType(), False),
    StructField("session_id",     StringType(),  False),
    StructField("event_type",     StringType(),  False),
    StructField("channel",        StringType(),  False),
    StructField("premium_amount", LongType(),    False),
])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Spark Kafka → ClickHouse backfill")
    p.add_argument("--topic", default="user_events")
    p.add_argument("--start-offsets",
                   help="'earliest' or per-partition JSON. Mutually exclusive with --start-timestamp.")
    p.add_argument("--end-offsets",
                   help="'latest' or per-partition JSON. Mutually exclusive with --end-timestamp.")
    p.add_argument("--start-timestamp",
                   help="ISO datetime, e.g. 2025-10-15T00:00:00. Applies to all partitions.")
    p.add_argument("--end-timestamp",
                   help="ISO datetime, e.g. 2025-10-16T00:00:00. Applies to all partitions.")
    p.add_argument("--target-table", default="raw_user_events")
    args = p.parse_args()

    if args.start_offsets and args.start_timestamp:
        p.error("specify either --start-offsets or --start-timestamp, not both")
    if args.end_offsets and args.end_timestamp:
        p.error("specify either --end-offsets or --end-timestamp, not both")
    if not args.start_offsets and not args.start_timestamp:
        args.start_offsets = "earliest"
    if not args.end_offsets and not args.end_timestamp:
        args.end_offsets = "latest"
    return args


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("spark-backfill")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def load_users_broadcast(spark: SparkSession) -> DataFrame:
    log.info("loading users dimension from MySQL")
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
    log.info(f"  loaded {n:,} users")
    return broadcast(df)


def kafka_read(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    reader = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", args.topic)
        .option("failOnDataLoss", "false")
    )

    if args.start_timestamp:
        ts_ms = int(datetime.fromisoformat(args.start_timestamp).timestamp() * 1000)
        reader = reader.option("startingTimestamp", str(ts_ms))
        log.info(f"  startingTimestamp = {args.start_timestamp} ({ts_ms} ms)")
    else:
        reader = reader.option("startingOffsets", args.start_offsets)
        log.info(f"  startingOffsets = {args.start_offsets}")

    if args.end_timestamp:
        ts_ms = int(datetime.fromisoformat(args.end_timestamp).timestamp() * 1000)
        reader = reader.option("endingTimestamp", str(ts_ms))
        log.info(f"  endingTimestamp = {args.end_timestamp} ({ts_ms} ms)")
    else:
        reader = reader.option("endingOffsets", args.end_offsets)
        log.info(f"  endingOffsets = {args.end_offsets}")

    return reader.load()


def main() -> int:
    args = parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info("=== spark-backfill starting ===")
    log.info(f"  topic={args.topic}  target=azki.{args.target_table}")

    raw = kafka_read(spark, args)
    n_messages = raw.count()
    log.info(f"read {n_messages:,} messages from Kafka")

    if n_messages == 0:
        log.warning("no messages in range; exiting")
        return 0

    parsed = (
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

    users_bc = load_users_broadcast(spark)
    enriched = (
        parsed.join(users_bc, on="user_id", how="left")
              .select(
                  "event_time", "user_id", "session_id", "event_type", "channel",
                  "premium_amount", "city", "device_type", "signup_date", "event_hash",
              )
    )

    n_rows = enriched.count()
    log.info(f"enriched {n_rows:,} rows → clickhouse.{args.target_table}")

    (
        enriched.write.format("jdbc")
        .mode("append")
        .option("url", CH_URL)
        .option("dbtable", args.target_table)
        .option("user", CH_USER)
        .option("password", CH_PASSWORD)
        .option("driver", CH_DRIVER)
        .option("batchsize", "5000")
        .option("isolationLevel", "NONE")
        .save()
    )

    log.info(f"=== spark-backfill done: {n_rows:,} rows written ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
