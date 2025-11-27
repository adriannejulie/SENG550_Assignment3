from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col
import os
import shutil
import glob

spark = SparkSession.builder \
    .appName("AggregateOrders") \
    .getOrCreate()


input_path = "../../part1/data/raw/*/orders_*.csv"
print(glob.glob(input_path))

df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

category_cols = df.columns[4:]

agg_exprs = [sum(col(c)).alias(c) for c in category_cols]

agg_df = df.groupBy("order_dow", "order_hour_of_day") \
           .agg(*agg_exprs) \
           .orderBy("order_dow", "order_hour_of_day")

output_dir = "../../data/processed/tmp_agg_output"
final_path = "../../data/processed/orders.csv"

os.makedirs(os.path.dirname(final_path), exist_ok=True)

agg_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_dir)

part_file = [f for f in os.listdir(output_dir) if f.startswith("part-") and f.endswith(".csv")][0]
shutil.move(os.path.join(output_dir, part_file), final_path)

shutil.rmtree(output_dir)

print(f"Aggregated sales saved to {final_path}")

spark.stop()
