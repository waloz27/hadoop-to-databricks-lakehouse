# Databricks notebook source
from pyspark.sql import functions as F
import time, statistics

(spark.table("lakehouse.silver.yellow_trips_clean")
    .repartition(400)
    .write.mode("overwrite")
    .saveAsTable("lakehouse.silver.trips_archivos_pequenos"))

spark.sql("DESCRIBE DETAIL lakehouse.silver.trips_archivos_pequenos") \
     .select("numFiles", "sizeInBytes").show()

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "false")

def consulta():
    return (spark.table("lakehouse.silver.trips_archivos_pequenos")
            .filter(F.col("pu_borough") == "Manhattan")
            .groupBy("pickup_date")
            .agg(F.count("*").alias("viajes"), F.avg("total_amount").alias("tarifa_prom")))

def medir(nombre, repeticiones=3):
    consulta().write.format("noop").mode("overwrite").save()  # calentamiento
    tiempos = []
    for _ in range(repeticiones):
        t0 = time.time()
        consulta().write.format("noop").mode("overwrite").save()
        tiempos.append(time.time() - t0)
    print(f"{nombre}: mediana {statistics.median(tiempos):.1f}s | corridas {[round(t, 1) for t in tiempos]}")

medir("Antes de OPTIMIZE")

# COMMAND ----------

medir("Antes de OPTIMIZE (5 corridas)", repeticiones=5)

# COMMAND ----------

spark.sql("OPTIMIZE lakehouse.silver.trips_archivos_pequenos").show(truncate=False)

spark.sql("DESCRIBE DETAIL lakehouse.silver.trips_archivos_pequenos") \
     .select("numFiles", "sizeInBytes").show()

# COMMAND ----------

medir("Después de OPTIMIZE (5 corridas)", repeticiones=5)

# COMMAND ----------

spark.sql("DROP TABLE lakehouse.silver.trips_archivos_pequenos")