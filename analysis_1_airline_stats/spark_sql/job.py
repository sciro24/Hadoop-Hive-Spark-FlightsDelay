#!/usr/bin/env python3
"""
Analisi 3.1 — Statistiche Compagnie Aeree
Tecnologia: Spark SQL 3.5.8
"""
import os
import sys
import time
from pathlib import Path
from pyspark.sql import SparkSession

# ─── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH   = str(PROJECT_ROOT / "data" / "cleaned" / "flight_data_2024_cleaned.csv")
OUTPUT_PATH  = str(PROJECT_ROOT / "results" / "analysis_1" / "spark_sql")

os.makedirs(OUTPUT_PATH, exist_ok=True)

# ─── SparkSession ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Analysis_3.1_AirlineStats_SparkSQL") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"Spark versione: {spark.version}")
print(f"Input: {INPUT_PATH}")

# ─── Caricamento ──────────────────────────────────────────────────────────────
start = time.time()

df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)
print(f"Righe caricate: {df.count():,}")

# ─── Registra vista temporanea ────────────────────────────────────────────────
df.createOrReplaceTempView("flights")

# ─── Query Analisi 3.1 ────────────────────────────────────────────────────────
# Per ogni (compagnia, aeroporto di partenza, mese):
# - numero voli
# - ritardo min/max/medio in arrivo
# - tasso di cancellazione
# - lista mesi operativi (collect_set aggregato per carrier+origin)

query_monthly = """
    SELECT
        op_unique_carrier                           AS carrier,
        origin,
        month,
        COUNT(*)                                    AS num_flights,
        ROUND(MIN(arr_delay), 2)                    AS min_arr_delay,
        ROUND(MAX(arr_delay), 2)                    AS max_arr_delay,
        ROUND(AVG(arr_delay), 2)                    AS avg_arr_delay,
        ROUND(SUM(CAST(cancelled AS DOUBLE)) / COUNT(*), 4) AS cancel_rate
    FROM flights
    WHERE op_unique_carrier IS NOT NULL
      AND origin IS NOT NULL
      AND month  IS NOT NULL
    GROUP BY op_unique_carrier, origin, month
    ORDER BY carrier, origin, month
"""

# ─── Query con lista mesi operativi aggregata per (carrier, origin) ───────────
query_with_months = """
    WITH monthly AS (
        SELECT
            op_unique_carrier   AS carrier,
            origin,
            month,
            COUNT(*)            AS num_flights,
            ROUND(MIN(arr_delay), 2)  AS min_arr_delay,
            ROUND(MAX(arr_delay), 2)  AS max_arr_delay,
            ROUND(AVG(arr_delay), 2)  AS avg_arr_delay,
            ROUND(SUM(CAST(cancelled AS DOUBLE)) / COUNT(*), 4) AS cancel_rate
        FROM flights
        WHERE op_unique_carrier IS NOT NULL
          AND origin IS NOT NULL
          AND month  IS NOT NULL
        GROUP BY op_unique_carrier, origin, month
    ),
    active_months AS (
        SELECT
            carrier,
            origin,
            ARRAY_JOIN(TRANSFORM(ARRAY_SORT(COLLECT_SET(month)), x -> CAST(x AS STRING)), ',') AS months_active
        FROM monthly
        GROUP BY carrier, origin
    )
    SELECT
        m.carrier,
        m.origin,
        m.month,
        m.num_flights,
        m.min_arr_delay,
        m.max_arr_delay,
        m.avg_arr_delay,
        m.cancel_rate,
        a.months_active
    FROM monthly m
    JOIN active_months a
      ON m.carrier = a.carrier AND m.origin = a.origin
    ORDER BY m.carrier, m.origin, m.month
"""

results = spark.sql(query_with_months)

# ─── Salvataggio ──────────────────────────────────────────────────────────────
results.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .csv(OUTPUT_PATH + "/raw")

# Rinomina part file in output.csv
import glob, shutil
part_files = glob.glob(f"{OUTPUT_PATH}/raw/part-*.csv")
if part_files:
    shutil.copy(part_files[0], f"{OUTPUT_PATH}/output.csv")

elapsed = round(time.time() - start, 2)

print(f"\nTempo di esecuzione: {elapsed}s")
print(f"Risultato: {OUTPUT_PATH}/output.csv")

# ─── Prime 10 righe ───────────────────────────────────────────────────────────
print("\n=== Prime 10 righe ===")
results.show(10, truncate=False)

spark.stop()