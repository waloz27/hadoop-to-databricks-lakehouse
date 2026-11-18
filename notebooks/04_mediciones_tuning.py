# Databricks notebook source
import time, statistics
from pyspark.sql import functions as F

silver = spark.table("lakehouse.silver.yellow_trips_clean")

def consulta():
    return (silver.groupBy("pu_zone", "do_zone")
            .agg(F.count("*").alias("viajes"), F.avg("total_amount").alias("tarifa_prom")))

def medir(nombre, repeticiones=3):
    tiempos = []
    for _ in range(repeticiones):
        t0 = time.time()
        consulta().write.format("noop").mode("overwrite").save()
        tiempos.append(time.time() - t0)
    print(f"{nombre}: mediana {statistics.median(tiempos):.1f}s | corridas {[round(t, 1) for t in tiempos]}")

# calentamiento: la primera lectura es en frío y no se compara
consulta().write.format("noop").mode("overwrite").save()

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "false")
for p in [4, 16, 200, 2000]:
    spark.conf.set("spark.sql.shuffle.partitions", str(p))
    medir(f"AQE apagado, shuffle.partitions={p}")

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
medir("AQE encendido, shuffle.partitions=200")