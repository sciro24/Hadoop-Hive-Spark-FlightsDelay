#!/usr/bin/env python3
"""
Analisi 3.3 — Ranking coppie compagnia-aeroporto
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
OUTPUT_PATH  = str(PROJECT_ROOT / "results" / "analysis_3" / "spark_sql")
os.makedirs(OUTPUT_PATH, exist_ok=True)

# ─── SparkSession ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Analysis_3.3_Ranking_SparkSQL") \
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

# ─── Query principale ─────────────────────────────────────────────────────────
query = """
    WITH carrier_stats AS (
        -- Statistiche per ogni (aeroporto, compagnia)
        SELECT
            origin,
            op_unique_carrier                               AS carrier,
            COUNT(*)                                        AS num_flights,
            ROUND(AVG(dep_delay), 4)                        AS avg_dep_delay,
            ROUND(AVG(arr_delay), 4)                        AS avg_arr_delay,
            ROUND(SUM(CAST(cancelled AS DOUBLE)) / COUNT(*), 4) AS cancel_rate
        FROM flights
        WHERE origin            IS NOT NULL
          AND op_unique_carrier IS NOT NULL
        GROUP BY origin, op_unique_carrier
    ),
    airport_avg AS (
        -- Media globale dep_delay per aeroporto (tutte le compagnie)
        SELECT
            origin,
            ROUND(AVG(dep_delay), 4) AS avg_dep_airport
        FROM flights
        WHERE origin IS NOT NULL
        GROUP BY origin
    ),
    joined AS (
        -- Join tra carrier_stats e airport_avg
        SELECT
            cs.origin,
            cs.carrier,
            cs.num_flights,
            cs.avg_dep_delay,
            cs.avg_arr_delay,
            cs.cancel_rate,
            aa.avg_dep_airport,
            ROUND(cs.avg_dep_delay - aa.avg_dep_airport, 4) AS dep_diff
        FROM carrier_stats cs
        JOIN airport_avg   aa ON cs.origin = aa.origin
    )
    -- Rank per aeroporto: dalla migliore (dep_delay più basso) alla peggiore
    SELECT
        origin,
        carrier,
        num_flights,
        avg_dep_delay,
        avg_arr_delay,
        cancel_rate,
        avg_dep_airport,
        dep_diff,
        RANK() OVER (
            PARTITION BY origin
            ORDER BY avg_dep_delay ASC
        ) AS rank
    FROM joined
    ORDER BY origin, rank
"""

result = spark.sql(query)

# ─── Salvataggio ──────────────────────────────────────────────────────────────
result.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .csv(f"{OUTPUT_PATH}/ranking_raw")

parts = glob.glob(f"{OUTPUT_PATH}/ranking_raw/part-*.csv")
if parts and "cleaned" in INPUT_PATH:
    shutil.copy(parts[0], f"{OUTPUT_PATH}/output.csv")
    print(f"Dataset completo rilevato. Risultati salvati in {OUTPUT_PATH}/output.csv")
elif parts:
    print(f"Dataset sample rilevato. Salto aggiornamento {OUTPUT_PATH}/output.csv")

shutil.rmtree(f"{OUTPUT_PATH}/ranking_raw", ignore_errors=True)

elapsed = round(time.time() - start, 2)
print(f"\nTempo di esecuzione: {elapsed}s")
print(f"Risultati in: {OUTPUT_PATH}")

# ─── Prime 10 righe ───────────────────────────────────────────────────────────
print("\n=== Prime 10 righe ===")
result.show(10, truncate=False)

spark.stop()