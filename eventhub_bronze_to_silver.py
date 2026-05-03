# Databricks notebook source
# CELL 0 — CONFIG (RUN FIRST ALWAYS)

import os
from dotenv import load_dotenv

# Load env variables
load_dotenv()

STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
STORAGE_KEY = os.getenv("STORAGE_KEY")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)
SILVER=f'abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net'

print("✅ Storage config set")

# COMMAND ----------

BRONZE_PATH = f'abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/stream_orders'

# COMMAND ----------

bronze_df = spark.readStream.format("delta").load(BRONZE_PATH)

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

preview_df = spark.read.format("delta").load(BRONZE_PATH)
display(preview_df)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

silver_df = bronze_df \
    .filter(col("quantity") > 0) \
    .filter(col("unit_price") > 0) \
    .withColumn("order_ts", col("timestamp").cast("timestamp")) \
    .withColumnRenamed("unit_price", "unit_price_inr") \
    .withColumnRenamed("total_amount", "total_amount_inr") \
    .drop("timestamp") \
    .withColumn("orders_loaded_at", current_timestamp()) \
    .select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "currency",
        "order_ts",
        "orders_loaded_at"
    )

# COMMAND ----------

SILVER_PATH = f'abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/orders'
CHECKPOINT_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/orders_stream"
silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start(SILVER_PATH)

# COMMAND ----------

display(spark.read.format("delta").load(SILVER_PATH))

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

