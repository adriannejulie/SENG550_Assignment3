from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col
import os
import shutil
import glob

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("AggregateOrders") \
    .getOrCreate()

# 2. Read all 7 CSV files
# Script is in processing/full/
input_path = "../../part1/data/raw/*/orders_*.csv"
print(glob.glob(input_path))

df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# 3. Identify category columns (all columns after the first 4)
category_cols = df.columns[4:]

# 4. Aggregate items sold by day_of_week, hour_of_day, and category
agg_exprs = [sum(col(c)).alias(c) for c in category_cols]

agg_df = df.groupBy("order_dow", "order_hour_of_day") \
           .agg(*agg_exprs) \
           .orderBy("order_dow", "order_hour_of_day")

# 5. Prepare output paths
output_dir = "../../data/processed/tmp_agg_output"
final_path = "../../data/processed/orders.csv"

# Ensure the final folder exists
os.makedirs(os.path.dirname(final_path), exist_ok=True)

# 6. Write temporary Spark CSV
agg_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_dir)

# 7. Move the single part file to the final CSV
part_file = [f for f in os.listdir(output_dir) if f.startswith("part-") and f.endswith(".csv")][0]
shutil.move(os.path.join(output_dir, part_file), final_path)

# 8. Clean up temporary folder
shutil.rmtree(output_dir)

print(f"Aggregated sales saved to {final_path}")

# Stop Spark session
spark.stop()
