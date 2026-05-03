# Databricks notebook source
# Config
import os
from dotenv import load_dotenv

# Load env variables
load_dotenv()

STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
STORAGE_KEY = os.getenv("STORAGE_KEY")
# Set access
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)
GOLD_CONTAINER = os.getenv("GOLD_CONTAINER", "gold")
# COMMAND ----------

GOLD_INPUT = f"abfss:// {GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/enriched_orders"

df = spark.read.format("delta").load(GOLD_INPUT)

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
df_clean = df.fillna({
    "city":"Unknown",
    "country":"Unknown",
    "loyalty_tier":"Standard",
    "condition":"Clear"
})

df_clean = df_clean.filter(
    (F.col("quantity") > 0) &
    (F.col("total_amount_inr") > 0)
)

# COMMAND ----------

dim_product = df_clean.select(
    "product_id",
    "product_name",
    "category",
    "subcategory",
    "price_inr",
    "supplier"
).dropDuplicates(["product_id"])

# COMMAND ----------

dim_product = dim_product.withColumn(
    "product_sk",
    F.monotonically_increasing_id()
)

# COMMAND ----------

GOLD_BASE = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
dim_product.write.format("delta") \
.mode("overwrite") \
.save(f"{GOLD_BASE}/dim_product")

# COMMAND ----------

dim_customer_stage = df_clean.select(
    "customer_id",
    F.col("name").alias("customer_name"),
    "city",
    "country",
    "loyalty_tier"
).dropDuplicates()

# COMMAND ----------

dim_customer = dim_customer_stage \
.withColumn(
 "customer_sk",
 F.monotonically_increasing_id()
).withColumn(
 "effective_date",
 F.current_date()
).withColumn(
 "end_date",
 F.lit(None).cast("date")
).withColumn(
 "is_current",
 F.lit(True)
)

# COMMAND ----------

dim_customer.write.format("delta") \
.mode("overwrite") \
.save(f"{GOLD_BASE}/dim_customer")

# COMMAND ----------

from delta.tables import DeltaTable

customer_dim = DeltaTable.forPath(
 spark,
 f"{GOLD_BASE}/dim_customer"
)

# COMMAND ----------

updates = dim_customer_stage.alias("src")

# COMMAND ----------

customer_dim.alias("tgt").merge(
 updates,
 """
 tgt.customer_id=src.customer_id
 AND tgt.is_current=true
 """
).whenMatchedUpdate(
 condition="tgt.city <> src.city",
 set={
   "end_date":"current_date()",
   "is_current":"false"
 }
).whenNotMatchedInsert(
 values={
   "customer_id":"src.customer_id",
   "customer_name":"src.customer_name",
   "city":"src.city",
   "country":"src.country",
   "loyalty_tier":"src.loyalty_tier",
   "effective_date":"current_date()",
   "end_date":"NULL",
   "is_current":"true"
 }
).execute()

# COMMAND ----------

dim_customer = spark.read.format("delta").load(
 f"{GOLD_BASE}/dim_customer"
).filter("is_current=true")

dim_product = spark.read.format("delta").load(
 f"{GOLD_BASE}/dim_product"
)

# COMMAND ----------

fact_orders = df_clean \
.join(
 dim_customer.select(
  "customer_id",
  "customer_sk"
 ),
 "customer_id"
) \
.join(
 dim_product.select(
  "product_id",
  "product_sk"
 ),
 "product_id"
) \
.select(
 "order_id",
 "customer_sk",
 "product_sk",

 "order_ts",

 "quantity",
 "total_amount_inr",
 "total_amount_usd",

 "temp_celsius",
 "humidity_pct",
 "condition",

 "currency"
)

# COMMAND ----------

fact_orders.write.format("delta") \
.mode("overwrite") \
.save(f"{GOLD_BASE}/fact_orders")

# COMMAND ----------

# POWER BI AGGREGEATES

# COMMAND ----------

from pyspark.sql import functions as F

customers_kpi = df.select(
F.countDistinct("customer_id")
.alias("total_customers")
)

customers_kpi.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("mart_segment_aov")

# COMMAND ----------

orders_kpi = df.select(
F.countDistinct("order_id")
.alias("total_orders")
)

orders_kpi.write \
.format("delta") \
.mode("overwrite") \
.save(f"{GOLD_BASE}/kpi_total_orders")

# COMMAND ----------

revenue_kpi = df.select(
F.round(
F.sum("total_amount_inr"),2
).alias("total_revenue_inr"),

F.round(
F.sum("total_amount_usd"),2
).alias("total_revenue_usd")
)

revenue_kpi.write \
.format("delta") \
.mode("overwrite") \
.save(f"{GOLD_BASE}/kpi_total_revenue")

# COMMAND ----------

# rfm calculation

# COMMAND ----------

# -----------------------------
# Reference date (latest order)
# -----------------------------

max_dt = df.select(
    F.max("order_ts")
).collect()[0][0]

# -----------------------------
# Customer level RFM metrics
# -----------------------------

rfm = df.groupBy(
    "customer_id",
    "name",
    "city"
).agg(

    F.max("order_ts").alias(
       "last_order_date"
    ),

    F.countDistinct("order_id").alias(
       "frequency"
    ),

    F.round(
      F.sum("total_amount_inr"),2
    ).alias(
      "monetary"
    )

)

# COMMAND ----------

# Recency in days
rfm = rfm.withColumn(
    "recency",
    F.datediff(
      F.lit(max_dt),
      F.col("last_order_date")
    )
)
# --------------------------------
# R,F,M scoring using quintiles
# --------------------------------

# Lower recency better => descending inverse scoring
w_rec = Window.orderBy(
    F.col("recency").desc()
)

w_freq = Window.orderBy(
    F.col("frequency")
)

w_mon = Window.orderBy(
    F.col("monetary")
)

rfm = rfm.withColumn(
    "R_score",
    6 - F.ntile(5).over(w_rec)
)

rfm = rfm.withColumn(
    "F_score",
    F.ntile(5).over(w_freq)
)

rfm = rfm.withColumn(
    "M_score",
    F.ntile(5).over(w_mon)
)

# COMMAND ----------

# Combined RFM score
rfm = rfm.withColumn(
    "rfm_score",
    F.concat(
      F.col("R_score"),
      F.col("F_score"),
      F.col("M_score")
    )
)

# COMMAND ----------

# --------------------------------
# Segment assignment
# --------------------------------

rfm = rfm.withColumn(
"segment",

F.when(
(F.col("R_score")>=4) &
(F.col("F_score")>=4) &
(F.col("M_score")>=4),
"Champions"
)

.when(
(F.col("R_score")>=3) &
(F.col("F_score")>=4),
"Loyal Customers"
)

.when(
(F.col("R_score")>=4) &
(F.col("F_score")>=2),
"Potential Loyalists"
)

.when(
(F.col("R_score")<=2) &
(F.col("F_score")>=3),
"At Risk"
)

.when(
(F.col("R_score")<=2) &
(F.col("F_score")<=2),
"Lost Customers"
)

.otherwise(
"Others"
)

)

display(rfm)

# COMMAND ----------

# join customer segments back to orders
orders_seg = df.join(
    rfm.select(
      "customer_id",
      "segment"
    ),
    "customer_id",
    "inner"
)


segment_aov = (
orders_seg
.groupBy("segment")
.agg(

    F.countDistinct("order_id")
      .alias("total_orders"),

    F.round(
      F.sum("total_amount_inr"),
      2
    ).alias("segment_revenue"),

    F.round(
      F.sum("total_amount_inr") /
      F.countDistinct("order_id"),
      2
    ).alias("aov_inr"),

    F.round(
      F.sum("total_amount_usd") /
      F.countDistinct("order_id"),
      2
    ).alias("aov_usd")

)
.orderBy(
 F.desc("aov_inr")
)
)

display(segment_aov)

# COMMAND ----------

segment_aov.write \
.format("delta") \
.mode("overwrite") \
.save(
f"{GOLD_BASE}/mart_segment_aov"
)

# COMMAND ----------

customers_city = (
df
.groupBy("city")
.agg(
   F.countDistinct(
      "customer_id"
   ).alias(
      "num_customers"
   )
)
.orderBy(
 F.desc("num_customers")
)
)

display(customers_city)

# COMMAND ----------

customers_city.write \
.format("delta") \
.mode("overwrite") \
.save(
f"{GOLD_BASE}/mart_customers_by_city"
)

# COMMAND ----------

city_prod = (
df.groupBy(
"city",
"product_id",
"product_name"
)
.agg(
F.sum("total_amount_inr")
.alias("product_revenue")
)
)

w=Window.partitionBy(
"city"
).orderBy(
F.desc("product_revenue")
)

top_products_city = (
city_prod
.withColumn(
"rank",
F.row_number().over(w)
)
.filter(
F.col("rank")==1
)
)

display(top_products_city)

# COMMAND ----------

today_kpi = (
df.filter(
F.to_date("order_ts")==F.current_date()
)
.agg(

F.countDistinct(
"order_id"
).alias(
"orders_today"
),

F.round(
F.sum("total_amount_inr"),
2
).alias(
"revenue_today_inr"
),

F.round(
F.sum("total_amount_usd"),
2
).alias(
"revenue_today_usd"
)

)
)

display(today_kpi)

# COMMAND ----------

today_kpi.write \
.format("delta") \
.mode("overwrite") \
.save(
f"{GOLD_BASE}/mart_today_kpis"
)

# COMMAND ----------

monthly_new_users = (
df
.withColumn(
"signup_date",
F.to_date("signup_date")
)

.filter(
F.col("signup_date") >=
F.add_months(
F.current_date(),
-12
)
)

.groupBy(
F.date_format(
"signup_date",
"yyyy-MM"
).alias("signup_month")
)

.agg(
F.countDistinct(
"customer_id"
).alias(
"new_customers"
)
)

.orderBy(
"signup_month"
)
)

display(monthly_new_users)

# COMMAND ----------

monthly_new_users.write \
.format("delta") \
.mode("overwrite") \
.save(
f"{GOLD_BASE}/mart_monthly_new_customers"
)

# COMMAND ----------

weather_product_rev = (
df.groupBy(
"condition",
"product_name"
)
.agg(
F.round(
F.sum("total_amount_inr"),
2
).alias(
"product_revenue"
)
)
)

w = Window.partitionBy(
"condition"
).orderBy(
F.desc("product_revenue")
)

top_weather_products = (
weather_product_rev
.withColumn(
"rank",
F.row_number().over(w)
)
.filter(
F.col("rank") == 1
)
)

display(top_weather_products)

# COMMAND ----------

weather_revenue=(
df.groupBy(
"condition"
)
.agg(
F.round(
F.sum("total_amount_inr"),
2
).alias(
"total_revenue_inr"
),

F.round(
F.sum("total_amount_usd"),
2
).alias(
"total_revenue_usd"
)
)
.orderBy(
F.desc("total_revenue_inr")
)
)

display(weather_revenue)

# COMMAND ----------

top_weather_products.write \
.format("delta") \
.mode("overwrite") \
.save(
f"{GOLD_BASE}/mart_weather_top_products"
)

# COMMAND ----------

weather_revenue.write \
.format("delta") \
.mode("overwrite") \
.save(
f"{GOLD_BASE}/mart_weather_total_revenue"
)

# COMMAND ----------

display(
dbutils.fs.ls(GOLD_BASE)
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

sales_cat = df_clean.groupBy(
 "category"
).agg(
 F.sum("total_amount_inr").alias(
  "revenue"
 )
)

# COMMAND ----------

weather_insight = df_clean.groupBy(
 "condition"
).agg(
 F.avg("quantity").alias("avg_demand")
).write.format("delta") \
.mode("overwrite") \
.save(f"{GOLD_BASE}/weather_insight")

# COMMAND ----------

loyalty_insight = df_clean.groupBy(
 "loyalty_tier"
).agg(
 F.sum("total_amount_inr").alias("sales")
)

# COMMAND ----------

loyalty_insight.display()

# COMMAND ----------

df_clean = df_clean.withColumn(
 "month",
 F.month("order_ts")
)

# COMMAND ----------

recommendations = df_clean.groupBy(
 "month",
 "city",
 "product_name",
 "condition"
).agg(
 F.sum("quantity").alias("sales")
)

# COMMAND ----------

recommendations.display()

# COMMAND ----------

recommendations.write \
.mode("overwrite") \
.save(
f"{GOLD_BASE}/festive_recommendations"
)

# COMMAND ----------

pipeline_log = spark.createDataFrame(
[
("fact_orders",fact_orders.count())
],
["table","row_count"]
)

pipeline_log.write.mode("append") \
.option("mergeSchema", "true")\
.save(f"{GOLD_BASE}/pipeline_logs")

# COMMAND ----------

spark.conf.set(
"spark.sql.adaptive.enabled",
"true"
)
fact_orders.cache()


# COMMAND ----------

fx_insight = df_clean.groupBy(
    F.date_trunc("month","order_ts").alias("month")
).agg(
    F.sum("total_amount_inr").alias("revenue_inr"),
    F.sum("total_amount_usd").alias("revenue_usd")
)

display(fx_insight)

# COMMAND ----------

city_usd = df_clean.groupBy("city").agg(
    F.sum("total_amount_usd").alias("usd_sales")
).orderBy(
    F.desc("usd_sales")
)

display(city_usd)

# COMMAND ----------

category_fx = df.groupBy(
 "category"
).agg(
 F.sum("total_amount_usd").alias("usd_revenue"),
 F.avg("total_amount_usd").alias("avg_order_usd")
)

display(category_fx)

# COMMAND ----------

fx_impact = df_clean.withColumn(
 "fx_difference",
 F.col("total_amount_inr") -
 (F.col("total_amount_usd")*83)
)

display(fx_impact)

# COMMAND ----------

premium_festive = df_clean.groupBy(
 "month","product_name"
).agg(
 F.sum("quantity").alias("sales"),
 F.sum("total_amount_usd").alias("usd_value")
).orderBy(
 F.desc("usd_value")
)

# COMMAND ----------

weather_spend = df_clean.groupBy(
 "condition"
).agg(
 F.avg("total_amount_usd").alias("avg_order_value_usd")
)

# COMMAND ----------

fx_sales_mart = df_clean.groupBy(
 "city","category"
).agg(
 F.sum("total_amount_usd").alias("usd_sales")
)

# COMMAND ----------

fx_sales_mart.write.mode("overwrite").save(
f"{GOLD_BASE}/fx_sales_mart"
)

# COMMAND ----------

fact_df = spark.read.format("delta").load(
"abfss://gold@customer360adls12.dfs.core.windows.net/fact_orders"
)

fact_df.write.mode("overwrite").saveAsTable("fact_orders")

# COMMAND ----------

cust_df = spark.read.format("delta").load(
"abfss://gold@customer360adls12.dfs.core.windows.net/dim_customer"
)

cust_df.write.mode("overwrite").saveAsTable("dim_customer")

# COMMAND ----------

prod_df = spark.read.format("delta").load(
"abfss://gold@customer360adls12.dfs.core.windows.net/dim_product"
)

prod_df.write.mode("overwrite").saveAsTable("dim_product")

# COMMAND ----------

city_df = spark.read.format("delta").load(
"abfss://gold@customer360adls12.dfs.core.windows.net/agg_city_sales"
)

city_df.write.mode("overwrite").saveAsTable(
"agg_city_sales"
)

# COMMAND ----------

weather_df = spark.read.format("delta").load(
"abfss://gold@customer360adls12.dfs.core.windows.net/weather_insight"
)

weather_df.write.mode("overwrite").saveAsTable(
"weather_insight"
)

# COMMAND ----------

fx_df = spark.read.format("delta").load(
"abfss://gold@customer360adls12.dfs.core.windows.net/fx_sales_mart"
)

fx_df.write.mode("overwrite").saveAsTable(
"fx_sales_mart"
)

# COMMAND ----------

rec_df = spark.read.format("delta").load(
"abfss://gold@customer360adls12.dfs.core.windows.net/festive_recommendations"
)

rec_df.write.mode("overwrite").saveAsTable(
"festive_recommendations"
)

# COMMAND ----------

log_df = spark.read.format("delta").load(
"abfss://gold@customer360adls12.dfs.core.windows.net/pipeline_logs"
)

log_df.write.mode("overwrite").saveAsTable(
"pipeline_logs"
)

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

record_count = df_clean.count()

audit_rows = [
Row(
job_name="silver_onwards",
records_processed=record_count,
status="SUCCESS"
)
]

audit_df = spark.createDataFrame(audit_rows) \
.withColumn("run_time", current_timestamp())

display(audit_df)

# COMMAND ----------

audit_df.write.format("delta") \
.mode("append") \
.save(f"{GOLD_BASE}/job_complete_logs")

# COMMAND ----------

display(
dbutils.fs.ls(GOLD_PATH)
)

# COMMAND ----------

