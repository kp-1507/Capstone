# Databricks notebook source
# Notebook: bronze_to_silver
# CELL 1 — Configuration

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

BRONZE_BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/batch"
SILVER_BASE = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
QUARANTINE = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/quarantine"
AUDIT_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/audit_log"

# COMMAND ----------

# CELL 2 — Load customers, clean, mask PII

from pyspark.sql import functions as F
from datetime import datetime

# Load from bronze (IMPORTANT: wildcard use karo)
customers_raw = spark.read.option("header", True).csv(f"{BRONZE_BASE}/customers.csv")

# ---------------------------
# PII MASKING
# ---------------------------

customers_clean = customers_raw \
    .withColumn(
        "email_masked",
        F.concat(
            F.lit("***@"),
            F.split(F.col("email"), "@").getItem(1)
        )
    ) \
    .withColumn(
        "phone_masked",
        F.regexp_replace(F.col("phone"), r"\d(?=\d{4})", "*")
    ) \
    .drop("email", "phone")  # remove original PII

# ---------------------------
# AUDIT COLUMN
# ---------------------------

customers_clean = customers_clean.withColumn(
    "customers_loaded_at",
    F.current_timestamp()   # ✅ better than datetime.utcnow()
)



# ---------------------------
# WRITE TO SILVER (DELTA)
# ---------------------------

customers_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{SILVER_BASE}/customers")

print(f"✅ Customers written: {customers_clean.count()} rows")

# COMMAND ----------

# CELL 3 — Load products (no PII, just clean)

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Load from bronze
products_raw = spark.read.option("header", True).csv(f"{BRONZE_BASE}/products.csv")

# ---------------------------
# CLEANING + TYPE CASTING
# ---------------------------

products_clean = products_raw \
    .withColumn("price_inr", F.col("price_inr").cast(DoubleType())) \
    .withColumn("cost_inr", F.col("cost_inr").cast(DoubleType())) \
    .withColumn("products_loaded_at", F.current_timestamp())   # ✅ fix

products_clean=products_clean.select(
    "product_id",
    "product_name",
    "category",
    "subcategory",
    "price_inr",
    "supplier",
    "products_loaded_at"
)

# ---------------------------
# WRITE TO SILVER (DELTA)
# ---------------------------

products_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{SILVER_BASE}/products")

print(f"✅ Products written: {products_clean.count()} rows")

# COMMAND ----------

# CELL 4 — Load orders, validate, quarantine bad data

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# ---------------------------
# LOAD FROM BRONZE
# ---------------------------

orders_raw = spark.read.option("header", True).csv(f"{BRONZE_BASE}/orders.csv")
orders_raw = orders_raw.withColumn(
    "order_ts",
    F.to_timestamp("order_date")
)

# ---------------------------
# TYPE CASTING + CLEANING
# ---------------------------

orders_typed = orders_raw \
    .withColumn("quantity", F.col("quantity").cast(IntegerType())) \
    .withColumn("unit_price_inr", F.col("unit_price_inr").cast(DoubleType())) \
    .withColumn("total_amount_inr", F.col("total_amount_inr").cast(DoubleType())) \
    .withColumn("order_ts", F.to_timestamp("order_date")) \
    .withColumn("orders_loaded_at", F.current_timestamp())\
    .drop("order_date")   # 🔥 THIS LINE FIXES EVERYTHING

orders_typed=orders_typed.select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "currency",
        "order_ts",
        "orders_loaded_at"
)

# ---------------------------
# GOOD DATA FILTER
# ---------------------------

good_orders = orders_typed.filter(
    (F.col("quantity") > 0) &
    (F.col("unit_price_inr") > 0) &
    (F.col("order_id").isNotNull()) &
    (F.col("customer_id").isNotNull())
)

# ---------------------------
# BAD DATA (ANTI JOIN - CORRECT WAY)
# ---------------------------

bad_orders = orders_typed.join(
    good_orders,
    on=["order_id"],
    how="left_anti"
)

# ---------------------------
# ADD AUDIT COLUMN
# ---------------------------

# good_orders = good_orders.withColumn("loaded_at", F.current_timestamp())

# ---------------------------
# WRITE GOOD → SILVER
# ---------------------------

good_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{SILVER_BASE}/orders")

# ---------------------------
# WRITE BAD → QUARANTINE
# ---------------------------

bad_orders = bad_orders \
    .withColumn("quarantine_reason", F.lit("failed_validation")) \
    .withColumn("quarantined_at", F.current_timestamp())

bad_orders.write \
    .format("delta") \
    .mode("append").option("mergeSchema", "true") \
    .save(QUARANTINE)

print(f"✅ Good orders: {good_orders.count()} | ❌ Bad orders: {bad_orders.count()}")

# COMMAND ----------

good_orders.display()

# COMMAND ----------

# CELL 5 — Write audit log (optimized)

from pyspark.sql import Row
from pyspark.sql import functions as F

# ---------------------------
# PRE-COMPUTE COUNTS (avoid multiple scans)
# ---------------------------

customers_in = customers_raw.count()
customers_out = customers_clean.count()

orders_in = orders_typed.count()
orders_good = good_orders.count()
orders_bad = bad_orders.count()

# ---------------------------
# CREATE AUDIT DATA
# ---------------------------

audit_rows = [
    Row(
        table='customers',
        records_in=customers_in,
        records_out=customers_out,
        bad_records=0
    ),
    Row(
        table='orders',
        records_in=orders_in,
        records_out=orders_good,
        bad_records=orders_bad
    )
]

audit_df = spark.createDataFrame(audit_rows) \
    .withColumn("run_time", F.current_timestamp())

# ---------------------------
# WRITE TO DELTA
# ---------------------------

audit_df.write \
    .format('delta') \
    .mode('append') \
    .save(AUDIT_PATH)

print("✅ Audit log written successfully")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

