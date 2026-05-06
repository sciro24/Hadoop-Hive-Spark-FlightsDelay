#!/usr/bin/env python3
"""
Analisi 3.2 — Report Ritardi per Aeroporto e Periodo Temporale
Tecnologia: Spark SQL 3.5.8
"""
import os
import glob
import shutil
import time
from pathlib import Path
from pyspark.sql import SparkSession

import sys
# ─── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH   = sys.argv[1] if len(sys.argv) > 1 else str(PROJECT_ROOT / "data" / "cleaned" / "flight_data_2024_cleaned.parquet")
OUTPUT_PATH  = str(PROJECT_ROOT / "results" / "analysis_2" / "spark_sql")
os.makedirs(OUTPUT_PATH, exist_ok=True)

# ─── SparkSession ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Analysis_3.2_DelayReport_SparkSQL") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"Spark versione: {spark.version}")
print(f"Input: {INPUT_PATH}")

start = time.time()

# ─── Caricamento ──────────────────────────────────────────────────────────────
df = spark.read.parquet(INPUT_PATH)
df.createOrReplaceTempView("flights")
print(f"Righe caricate: {df.count():,}")

# ─── Query Unificata e Professionale ──────────────────────────────────────────
query = """
    WITH 
    -- 1. Calcolo Fasce di Ritardo
    bands AS (
        SELECT
            origin,
            month,
            CASE
                WHEN dep_delay < 15              THEN 'low'
                WHEN dep_delay BETWEEN 15 AND 60 THEN 'medium'
                WHEN dep_delay > 60              THEN 'high'
            END                             AS delay_band,
            COUNT(*)                        AS num_flights,
            ROUND(AVG(COALESCE(dep_delay, 0.0)), 2) AS avg_dep,
            ROUND(AVG(COALESCE(arr_delay, 0.0)), 2) AS avg_arr
        FROM flights
        WHERE origin IS NOT NULL AND month IS NOT NULL AND dep_delay IS NOT NULL
        GROUP BY origin, month, 
            CASE
                WHEN dep_delay < 15              THEN 'low'
                WHEN dep_delay BETWEEN 15 AND 60 THEN 'medium'
                WHEN dep_delay > 60              THEN 'high'
            END
    ),
    -- 2. Calcolo Cause (Top 3)
    causes_all AS (
        SELECT origin, month, 'carrier' AS cause, AVG(carrier_delay) AS am FROM flights WHERE carrier_delay > 0 GROUP BY origin, month
        UNION ALL
        SELECT origin, month, 'weather' AS cause, AVG(weather_delay) AS am FROM flights WHERE weather_delay > 0 GROUP BY origin, month
        UNION ALL
        SELECT origin, month, 'nas'     AS cause, AVG(nas_delay)     AS am FROM flights WHERE nas_delay     > 0 GROUP BY origin, month
        UNION ALL
        SELECT origin, month, 'security' AS cause, AVG(security_delay) AS am FROM flights WHERE security_delay > 0 GROUP BY origin, month
        UNION ALL
        SELECT origin, month, 'late_aircraft' AS cause, AVG(late_aircraft_delay) AS am FROM flights WHERE late_aircraft_delay > 0 GROUP BY origin, month
    ),
    causes_ranked AS (
        SELECT origin, month, cause,
               ROW_NUMBER() OVER (PARTITION BY origin, month ORDER BY am DESC) as rnk
        FROM causes_all
    ),
    causes_pivoted AS (
        SELECT 
            origin, month,
            MAX(CASE WHEN rnk = 1 THEN cause END) as top_cause_1,
            MAX(CASE WHEN rnk = 2 THEN cause END) as top_cause_2,
            MAX(CASE WHEN rnk = 3 THEN cause END) as top_cause_3
        FROM causes_ranked
        WHERE rnk <= 3
        GROUP BY origin, month
    )
    -- 3. Join Finale
    SELECT 
        b.origin, b.month, b.delay_band, b.num_flights, b.avg_dep, b.avg_arr,
        COALESCE(c.top_cause_1, 'none') as top_cause_1,
        COALESCE(c.top_cause_2, 'none') as top_cause_2,
        COALESCE(c.top_cause_3, 'none') as top_cause_3
    FROM bands b
    LEFT JOIN causes_pivoted c ON b.origin = c.origin AND b.month = c.month
    ORDER BY b.origin, b.month, b.delay_band
"""

results_unified = spark.sql(query)

# ─── Salvataggio ──────────────────────────────────────────────────────────────
results_unified.orderBy("origin", "month", "delay_band") \
    .coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .csv(f"{OUTPUT_PATH}/temp")

parts = glob.glob(f"{OUTPUT_PATH}/temp/part-*.csv")
if parts and "cleaned" in INPUT_PATH:
    shutil.copy(parts[0], f"{OUTPUT_PATH}/output.csv")
    print(f"Dataset completo rilevato. Risultati salvati in {OUTPUT_PATH}/output.csv")
elif parts:
    print(f"Dataset sample rilevato. Salto aggiornamento {OUTPUT_PATH}/output.csv")

shutil.rmtree(f"{OUTPUT_PATH}/temp", ignore_errors=True)

elapsed = round(time.time() - start, 2)
print(f"\nTempo di esecuzione: {elapsed}s")
print(f"Risultati in: {OUTPUT_PATH}")

# ─── Prime 10 righe ───────────────────────────────────────────────────────────
print("\n=== Prime 10 righe unificate ===")
results_unified.show(10, truncate=False)

spark.stop()