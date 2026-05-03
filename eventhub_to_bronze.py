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

print("✅ Storage config set")

# COMMAND ----------


EH_CONN_STR = os.getenv("EVENTHUB_CONNECTION_STRING")
EH_NAME = os.getenv("EVENTHUB_NAME")
BRONZE_PATH = f'abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/stream_orders'
CHECKPOINT = f'abfss://checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze_stream'

# COMMAND ----------

spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")

# COMMAND ----------

# connection string
connection_str1 = "Endpoint=sb://customer360adls.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=nqlPEEupznyVZ99t0uPyiN4xenNQy6gGJ+AEhKMAYCc=;EntityPath=customer360"

# SASL config
eh_sasl1 = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{connection_str1}";'

# read stream
df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "customer360adls.servicebus.windows.net:9093") \
  .option("subscribe", "customer360") \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.sasl.jaas.config", eh_sasl1) \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss","false") \
  .load()

# COMMAND ----------

from pyspark.sql.functions import col

decoded_df = df.selectExpr("CAST(value AS STRING) as json_data")

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("city", StringType()),
    StructField("timestamp", StringType()),
    StructField("currency", StringType())
])

# COMMAND ----------

from pyspark.sql.functions import from_json

parsed_df = decoded_df \
    .withColumn("data", from_json(col("json_data"), schema)) \
    .select("data.*")

# COMMAND ----------

checkpoint_path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/stream_orders"
bronze_path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/stream_orders"

bronze_query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .start(bronze_path)

# COMMAND ----------

spark.read.format("delta").load(bronze_path).display()

# COMMAND ----------

parsed_df.printSchema()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

