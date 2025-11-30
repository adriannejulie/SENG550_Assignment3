from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, lit
import os
import shutil
import glob

spark = SparkSession.builder \
    .appName("AggregateOrders") \
    .getOrCreate()

input_path = "../../part1/data/raw/*/orders_*.csv"
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# All category/count columns (after the first 3)
category_cols = df.columns[3:]

# Aggregate
agg_exprs = [sum(col(c)).alias(c) for c in category_cols]

agg_df = df.groupBy("order_dow", "order_hour_of_day") \
           .agg(*agg_exprs)

# Now UNPIVOT
# Convert from wide → long format
long_rows = []

for c in category_cols:
    long_rows.append(
        agg_df.select(
            "order_dow",
            "order_hour_of_day",
            lit(c).alias("category"),
            col(c).alias("count")
        )
    )

result_df = long_rows[0]
for r in long_rows[1:]:
    result_df = result_df.unionByName(r)

# Order nicely
result_df = result_df.orderBy("order_dow", "order_hour_of_day", "category")

# Save
output_dir = "../../data/processed/tmp_agg_output"
final_path = "../../data/processed/orders.csv"

os.makedirs(os.path.dirname(final_path), exist_ok=True)

result_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_dir)

part_file = [f for f in os.listdir(output_dir) if f.startswith("part-")][0]
shutil.move(os.path.join(output_dir, part_file), final_path)

shutil.rmtree(output_dir)

spark.stop()
