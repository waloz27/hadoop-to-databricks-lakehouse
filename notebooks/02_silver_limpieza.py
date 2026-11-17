# Databricks notebook source
from pyspark.sql import functions as F

bronze = spark.table("lakehouse.bronze.yellow_trips")
bronze.printSchema()

# COMMAND ----------

total_bronze = bronze.count()

limpio = (
    bronze
    .filter(F.col("tpep_pickup_datetime").isNotNull() & F.col("tpep_dropoff_datetime").isNotNull())
    .withColumn("duracion_min",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60)
    .filter((F.col("tpep_pickup_datetime") >= "2026-01-01") & (F.col("tpep_pickup_datetime") < "2026-03-01"))
    .filter(F.col("duracion_min").between(1, 180))
    .filter(F.col("trip_distance").between(0.1, 100))
    .filter((F.col("fare_amount") > 0) & (F.col("total_amount") > 0))
    .filter(F.col("passenger_count").isNull() | F.col("passenger_count").between(1, 6))
)

# COMMAND ----------

zonas = spark.table("lakehouse.bronze.taxi_zones")

pu = zonas.select(F.col("LocationID").alias("PULocationID"),
                  F.col("Borough").alias("pu_borough"), F.col("Zone").alias("pu_zone"))
do = zonas.select(F.col("LocationID").alias("DOLocationID"),
                  F.col("Borough").alias("do_borough"), F.col("Zone").alias("do_zone"))

silver = (
    limpio.join(pu, "PULocationID", "left").join(do, "DOLocationID", "left")
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .select("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
            "trip_distance", "PULocationID", "pu_borough", "pu_zone",
            "DOLocationID", "do_borough", "do_zone", "payment_type",
            "fare_amount", "tip_amount", "total_amount",
            "duracion_min", "pickup_date", "pickup_hour")
)

silver.write.mode("overwrite").saveAsTable("lakehouse.silver.yellow_trips_clean")

# COMMAND ----------

total_silver = spark.table("lakehouse.silver.yellow_trips_clean").count()
descartadas = total_bronze - total_silver
print(f"Bronze: {total_bronze:,} | Silver: {total_silver:,} | Descartadas: {descartadas:,} ({descartadas/total_bronze:.2%})")

# COMMAND ----------

spark.sql("SELECT * FROM lakehouse.silver.yellow_trips_clean").show()

# COMMAND ----------

total_bronze = spark.table("lakehouse.bronze.yellow_trips").count()
total_silver = spark.table("lakehouse.silver.yellow_trips_clean").count()
descartadas = total_bronze - total_silver
print(f"Bronze: {total_bronze:,} | Silver: {total_silver:,} | Descartadas: {descartadas:,} ({descartadas/total_bronze:.2%})")