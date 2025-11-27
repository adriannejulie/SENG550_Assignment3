from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col
import redis
import os
import glob
import shutil

RAW_PATH = "../../data/incremental/raw/*/"
PROCESSED_FILE = "../../data/incremental/processed/orders.csv"
TMP_DIR = "../../data/incremental/processed/tmp/"

def get_last_processed_day(r):
    """Retrieve last processed day from Redis (0 if nothing processed)."""
    value = r.get("last_processed_day")
    return int(value) if value else 0


def set_last_processed_day(r, day):
    r.set("last_processed_day", day)


def get_available_days():
    """Return sorted list of integer folder names."""
    folders = glob.glob("../../data/incremental/raw/*")
    return sorted([int(os.path.basename(f)) for f in folders])


def main():

    spark = SparkSession.builder.appName("IncrementalOrderAggregation").getOrCreate()

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    last_processed = get_last_processed_day(r)
    print(f"Last processed day in Redis: {last_processed}")

    available_days = get_available_days()
    new_days = [d for d in available_days if d > last_processed]

    print(f"Available days: {available_days}")
    print(f"New days to process: {new_days}")

    if not new_days:
        print("No new data to process.")
        spark.stop()
        return

    input_paths = [f"../../data/incremental/raw/{day}/orders_*.csv" for day in new_days]

    df = spark.read.option("header", True).option("inferSchema", True).csv(input_paths)

    category_cols = df.columns[4:]
    agg_exprs = [sum(col(c)).alias(c) for c in category_cols]

    agg_df = df.groupBy("order_dow", "order_hour_of_day") \
               .agg(*agg_exprs) \
               .orderBy("order_dow", "order_hour_of_day")

    os.makedirs(os.path.dirname(PROCESSED_FILE), exist_ok=True)

    if os.path.exists(PROCESSED_FILE):
        existing = spark.read.option("header", True).csv(PROCESSED_FILE)
        agg_df = existing.unionByName(agg_df)

    agg_df.coalesce(1).write.mode("overwrite").option("header", True).csv(TMP_DIR)

    part_file = [f for f in os.listdir(TMP_DIR) if f.startswith("part-")][0]
    shutil.move(os.path.join(TMP_DIR, part_file), PROCESSED_FILE)
    shutil.rmtree(TMP_DIR)

    print(f"Processed new days: {new_days}")

    set_last_processed_day(r, max(new_days))

    spark.stop()


if __name__ == "__main__":
    main()
