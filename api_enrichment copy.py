# Databricks notebook source
# Config
import os
from dotenv import load_dotenv

# Load env variables
load_dotenv()

STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
STORAGE_KEY = os.getenv("STORAGE_KEY")
EXCHANGERATE_KEY = os.getenv("EXCHANGERATE_KEY")

# Set access
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

# Paths
SILVER=f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
CUSTOMERS_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/customers"
PRODUCTS_PATH  = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/products"
ORDERS_PATH    = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/orders"

GOLD_PATH = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/enriched_orders"

# COMMAND ----------

customers_df = spark.read.format("delta").load(CUSTOMERS_PATH)
customers_df = customers_df.withColumnRenamed("city", "customer_city")

display(customers_df)

# COMMAND ----------

orders_df = spark.read.format("delta").load(ORDERS_PATH)
products_df = spark.read.format("delta").load(PRODUCTS_PATH)

# COMMAND ----------

display(orders_df)

# COMMAND ----------

df = orders_df \
    .join(customers_df, on="customer_id", how="left") \
    .join(products_df, on="product_id", how="left")

# COMMAND ----------

display(df)

# COMMAND ----------

import pyspark.sql.functions as F
df=df.withColumn("total_amount", F.round(F.col("quantity") * F.col("price_inr"), 2))

# COMMAND ----------

df = df.withColumnRenamed("total_amount", "total_amount_inr")

# COMMAND ----------

display(df)

# COMMAND ----------

cities = [row["customer_city"] for row in df.select("customer_city").distinct().collect()]

# COMMAND ----------

print(cities)

# COMMAND ----------

WEATHER_HIST_PATH=f"{SILVER}/dim_weather_history"

# COMMAND ----------

# distinct city + date only
city_dates = (
    df
    .withColumn("order_date", F.to_date("order_ts"))
    .select("customer_city","order_date")
    .distinct()
)

# COMMAND ----------

print(city_dates)

# COMMAND ----------

try:

    existing_weather = spark.read.format("delta") \
        .load(WEATHER_HIST_PATH)

    # only missing combinations
    missing = city_dates.join(
        existing_weather.select(
            "customer_city",
            "order_date"
        ),
        ["customer_city","order_date"],
        "left_anti"
    ).limit(100)

except:
    print("First run — full load")
    missing=city_dates.limit(100)


rows=missing.collect()

print(f"Need API calls for {len(rows)} new combinations")

# COMMAND ----------

coords = {
    "Delhi": (28.6139,77.2090),
    "Mumbai": (19.0760,72.8777),
    "Bengaluru": (12.9716,77.5946),
    "Hyderabad": (17.3850,78.4867),
    "Chennai": (13.0827,80.2707),
    "Pune": (18.5204,73.8567),
    "Kolkata": (22.5726,88.3639)
}

# COMMAND ----------

import requests
import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

weather_data=[]

for row in rows:

    city = row["customer_city"]
    dt   = str(row["order_date"])

    if city not in coords:
        continue

    lat,lon = coords[city]

    try:

        url = (
          f"https://archive-api.open-meteo.com/v1/archive"
          f"?latitude={lat}"
          f"&longitude={lon}"
          f"&start_date={dt}"
          f"&end_date={dt}"
          f"&daily=temperature_2m_max,"
          f"relative_humidity_2m_mean,"
          f"weather_code"
          f"&timezone=auto"
        )

        r=requests.get(url,timeout=20)

        if r.status_code==200:

            d=r.json()

            # simple weather code mapping
            code=d["daily"]["weather_code"][0]

            if code==0:
                cond="Clear"
            elif code in [1,2,3]:
                cond="Cloudy"
            elif code in [51,61,63]:
                cond="Rain"
            else:
                cond="Other"

            weather_data.append({
                "city": city,
                "temp_celsius": float(
                    d["daily"]["temperature_2m_max"][0]
                ),
                "humidity_pct": int(
                    d["daily"]["relative_humidity_2m_mean"][0]
                ),
                "condition": cond,
                "fetched_at": pd.Timestamp.utcnow()
            })

        else:
            print(f"WARNING {city} {dt} -> {r.status_code}")

    except Exception as e:
        print(f"ERROR {city} {dt} -> {e}")


weather_df = spark.createDataFrame(
    pd.DataFrame(weather_data)
)

display(weather_df)

# COMMAND ----------

FX_DIM_PATH=f"{SILVER}/dim_fx_history"


# COMMAND ----------

# ---------------------------------
# distinct order dates only
# ---------------------------------

order_dates=(
df
.withColumn(
   "order_date",
   F.to_date("order_ts")
)
.select("order_date")
.distinct()
)

# COMMAND ----------

try:

    existing_fx=spark.read.format("delta") \
       .load(FX_DIM_PATH)

    missing_dates=order_dates.join(
       existing_fx.select("rate_date"),
       order_dates.order_date==
       existing_fx.rate_date,
       "left_anti"
    )

except:
    print("First run bootstrap")
    missing_dates=order_dates


if missing_dates.count()==0:
    print("No new dates to fetch")

else:

    # -----------------------------------
    # 3 Find min and max missing dates
    # -----------------------------------

    d=missing_dates.selectExpr(
      "min(order_date) as mn",
      "max(order_date) as mx"
    ).first()

    start_date=str(d["mn"])
    end_date=str(d["mx"])

    print(
      f"Fetching FX from {start_date} to {end_date}"
    )

# COMMAND ----------

url=(
      f"https://api.frankfurter.app/"
      f"{start_date}..{end_date}"
      f"?from=INR&to=USD"
    )

r=requests.get(
      url,
      timeout=60
)

data=r.json()["rates"]

fx_rows=[]

    # only keep dates we actually need
missing_set=set(
      str(x["order_date"])
      for x in missing_dates.collect()
)

for dt,val in data.items():

        if dt in missing_set:

            fx_rows.append({
             "from_currency":"INR",
             "to_currency":"USD",
             "rate":float(val["USD"]),
             "rate_date":dt,
             "fetched_at":pd.Timestamp.utcnow()
            })

# COMMAND ----------

print(fx_rows)

# COMMAND ----------

if len(fx_rows)>0:

        new_fx_df=spark.createDataFrame(
           pd.DataFrame(fx_rows)
        )

        new_fx_df.write \
          .format("delta") \
          .mode("append") \
          .save(FX_DIM_PATH)

# COMMAND ----------

# -----------------------------------
# 6 Read full dimension
# -----------------------------------

fx_df=spark.read.format("delta") \
.load(FX_DIM_PATH)

display(fx_df)

# COMMAND ----------

df = df.withColumnRenamed("customer_city", "city")

# COMMAND ----------

gold_df = df.join(weather_df, on="city", how="left")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

fx_df = fx_df.withColumn("fetched_at", current_timestamp())

# COMMAND ----------

from pyspark.sql import functions as F
usd_rate = fx_df.filter(F.col("to_currency")=="USD") \
    .select("rate").first()[0]

gold_df = gold_df.withColumn(
    "total_amount_inr",
    F.round(F.col("quantity") * F.col("price_inr"), 2)
).withColumn(
    "total_amount_usd",
    F.round(F.col("total_amount_inr") * usd_rate, 2)
)

# COMMAND ----------

gold_df.display()

# COMMAND ----------

gold_df.printSchema()

# COMMAND ----------

gold_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(GOLD_PATH)

# COMMAND ----------

