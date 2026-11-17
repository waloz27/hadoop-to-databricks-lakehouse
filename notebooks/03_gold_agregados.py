# Databricks notebook source
# MAGIC %md
# MAGIC Resumen diario por distrito

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table("lakehouse.silver.yellow_trips_clean")

resumen_diario = (
    silver.groupBy("pickup_date", "pu_borough")
    .agg(
        F.count("*").alias("viajes"),
        F.round(F.avg("trip_distance"), 2).alias("distancia_prom_millas"),
        F.round(F.avg("duracion_min"), 1).alias("duracion_prom_min"),
        F.round(F.sum("total_amount"), 2).alias("ingreso_total"),
        F.round(F.avg(F.when((F.col("payment_type") == 1) & (F.col("fare_amount") > 0),
                             F.col("tip_amount") / F.col("fare_amount"))), 3).alias("propina_prom_pct"),
    )
)

resumen_diario.write.mode("overwrite").saveAsTable("lakehouse.gold.resumen_diario_borough")

# COMMAND ----------

# MAGIC %md
# MAGIC Zonas con mas recogidas

# COMMAND ----------

top_zonas = (
    silver.groupBy("pu_borough", "pu_zone")
    .agg(F.count("*").alias("viajes"), F.round(F.sum("total_amount"), 2).alias("ingreso_total"))
    .orderBy(F.desc("viajes"))
)

top_zonas.write.mode("overwrite").saveAsTable("lakehouse.gold.zonas_top_recogida")

# COMMAND ----------

# MAGIC %md
# MAGIC Verificar

# COMMAND ----------

spark.sql("SELECT * FROM lakehouse.gold.zonas_top_recogida LIMIT 10").show(truncate=False)
spark.sql("SELECT count(*) AS filas FROM lakehouse.gold.resumen_diario_borough").show()