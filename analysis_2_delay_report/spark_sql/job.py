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

# ─── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH   = str(PROJECT_ROOT / "data" / "cleaned" / "flight_data_2024_cleaned.csv")
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
df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)
df.createOrReplaceTempView("flights")
print(f"Righe caricate: {df.count():,}")

# ─── Query 1 — Fasce di ritardo per (aeroporto, mese) ────────────────────────
query_bands = """
    SELECT
        origin,
        month,
        CASE
            WHEN dep_delay IS NULL           THEN 'unknown'
            WHEN dep_delay < 15              THEN 'low'
            WHEN dep_delay BETWEEN 15 AND 60 THEN 'medium'
            WHEN dep_delay > 60              THEN 'high'
            ELSE 'unknown'
        END                             AS delay_band,
        COUNT(*)                        AS num_flights,
        ROUND(AVG(dep_delay), 2)        AS avg_dep_delay,
        ROUND(AVG(arr_delay), 2)        AS avg_arr_delay
    FROM flights
    WHERE origin IS NOT NULL
      AND month  IS NOT NULL
    GROUP BY origin, month,
        CASE
            WHEN dep_delay IS NULL           THEN 'unknown'
            WHEN dep_delay < 15              THEN 'low'
            WHEN dep_delay BETWEEN 15 AND 60 THEN 'medium'
            WHEN dep_delay > 60              THEN 'high'
            ELSE 'unknown'
        END
    ORDER BY origin, month, delay_band
"""

# ─── Query 2 — Top 3 cause di ritardo per (aeroporto, mese) ──────────────────
query_causes = """
    WITH causes_raw AS (
        SELECT origin, month, 'carrier_delay'       AS cause, AVG(carrier_delay)       AS avg_minutes FROM flights WHERE carrier_delay       > 0 GROUP BY origin, month
        UNION ALL
        SELECT origin, month, 'weather_delay'       AS cause, AVG(weather_delay)       AS avg_minutes FROM flights WHERE weather_delay       > 0 GROUP BY origin, month
        UNION ALL
        SELECT origin, month, 'nas_delay'           AS cause, AVG(nas_delay)           AS avg_minutes FROM flights WHERE nas_delay           > 0 GROUP BY origin, month
        UNION ALL
        SELECT origin, month, 'security_delay'      AS cause, AVG(security_delay)      AS avg_minutes FROM flights WHERE security_delay      > 0 GROUP BY origin, month
        UNION ALL
        SELECT origin, month, 'late_aircraft_delay' AS cause, AVG(late_aircraft_delay) AS avg_minutes FROM flights WHERE late_aircraft_delay > 0 GROUP BY origin, month
    ),
    ranked AS (
        SELECT
            origin,
            month,
            cause,
            ROUND(avg_minutes, 4)   AS avg_minutes,
            RANK() OVER (PARTITION BY origin, month ORDER BY avg_minutes DESC) AS rank_pos
        FROM causes_raw
    )
    SELECT origin, month, cause, avg_minutes, rank_pos
    FROM ranked
    WHERE rank_pos <= 3
    ORDER BY origin, month, rank_pos
"""

results_bands  = spark.sql(query_bands)
results_causes = spark.sql(query_causes)

# ─── Salvataggio ──────────────────────────────────────────────────────────────
for results, folder, outfile in [
    (results_bands,  "delay_report_raw", "output_delay_report.csv"),
    (results_causes, "delay_causes_raw", "output_delay_causes.csv"),
]:
    results.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", "|") \
        .csv(f"{OUTPUT_PATH}/{folder}")
    parts = glob.glob(f"{OUTPUT_PATH}/{folder}/part-*.csv")
    if parts:
        shutil.copy(parts[0], f"{OUTPUT_PATH}/{outfile}")

elapsed = round(time.time() - start, 2)
print(f"\nTempo di esecuzione: {elapsed}s")
print(f"Risultati in: {OUTPUT_PATH}")

# ─── Prime 10 righe ───────────────────────────────────────────────────────────
print("\n=== Prime 10 righe delay_report ===")
results_bands.show(10, truncate=False)

print("\n=== Prime 10 righe delay_causes ===")
results_causes.show(10, truncate=False)

spark.stop()