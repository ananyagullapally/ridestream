from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
from pyspark.sql.functions import col, window, count, sum
from config import KAFKA_BOOTSTRAP_SERVERS
from config import POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from surge_logic import detect_surge
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

detect_surge_udf = udf(detect_surge, BooleanType())

# ---------------------------------------------------
# Write to Postgres (foreachBatch)
# ---------------------------------------------------

def write_to_postgres(df, epoch_id):

    print(f"\n=== New Batch {epoch_id} ===")

    count_rows = df.count()
    print("Row count:", count_rows)

    if count_rows == 0:
        print("No rows to write.")
        return

    df.show(truncate=False)

    rows = df.collect()

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

    cur = conn.cursor()

    print("Columns coming from Spark:", df.columns)

    for row in rows:

        try:

            window_start = row["window_start"]
            window_end = row["window_end"]
            city = row["city"]
            rides_per_window = row["rides_per_window"]
            revenue_per_window = row["revenue_per_window"]
            surge_active = row["surge_active"]

            cur.execute("""
                INSERT INTO city_minute_metrics (
                    window_start,
                    window_end,
                    city,
                    rides_per_window,
                    revenue_per_window,
                    surge_active
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (window_start, city)
                DO UPDATE SET
                    window_end = EXCLUDED.window_end,
                    rides_per_window = EXCLUDED.rides_per_window,
                    revenue_per_window = EXCLUDED.revenue_per_window,
                    surge_active = EXCLUDED.surge_active;
            """, (
                window_start,
                window_end,
                city,
                rides_per_window,
                revenue_per_window,
                surge_active
            ))

        except Exception as e:

            print("\nERROR WRITING ROW")
            print("Row data:", row)
            print("Exception:", e)

            raise

    conn.commit()
    cur.close()
    conn.close()

    print("Batch written to Postgres")


# ---------------------------------------------------
# Spark Session
# ---------------------------------------------------

spark = SparkSession.builder \
    .appName("Ride Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# ---------------------------------------------------
# Kafka Source
# ---------------------------------------------------

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "ride_events") \
    .option("startingOffsets", "latest") \
    .load()


# ---------------------------------------------------
# Parse JSON
# ---------------------------------------------------

schema = StructType([
    StructField("ride_id", StringType()),
    StructField("city", StringType()),
    StructField("fare", DoubleType()),
    StructField("event_time", StringType())
])

parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("event_time")))


# ---------------------------------------------------
# Watermark + Window Aggregation
# ---------------------------------------------------

aggregated_df = (
    parsed_df
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "2 minutes", "30 seconds"),
        col("city")
    )
    .agg(
        count("*").alias("rides_per_window"),
        sum("fare").alias("revenue_per_window")
    )
    .withColumn(
        "surge_active",
        detect_surge_udf(col("rides_per_window"))
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("city"),
        col("rides_per_window"),
        col("revenue_per_window"),
        col("surge_active")
    )
)


# ---------------------------------------------------
# Start Streaming Query
# ---------------------------------------------------

query = (
    aggregated_df.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("update")
    .option("checkpointLocation", "checkpoint")
    .trigger(processingTime="10 seconds")
    .start()
)

print("STREAM STARTED")

query.awaitTermination()
