# Databricks notebook source
from pyspark.sql.functions import col

ruta = "/Volumes/lakehouse/bronze/landing/"

viajes = (
    spark.read.parquet(ruta + "yellow_tripdata_2026-*.parquet")
    .select("*", col("_metadata.file_name").alias("_source_file"))
)

viajes.write.mode("overwrite").saveAsTable("lakehouse.bronze.yellow_trips")

# COMMAND ----------

zonas = (
    spark.read.option("header", True).option("inferSchema", True)
    .csv(ruta + "taxi_zone_lookup.csv")
)
zonas.write.mode("overwrite").saveAsTable("lakehouse.bronze.taxi_zones")

# COMMAND ----------

spark.sql("SELECT count(*) AS filas FROM lakehouse.bronze.yellow_trips").show()
spark.sql("DESCRIBE DETAIL lakehouse.bronze.yellow_trips").select("format", "numFiles", "sizeInBytes").show()
spark.table("lakehouse.bronze.taxi_zones").show(5)