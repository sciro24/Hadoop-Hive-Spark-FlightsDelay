#!/usr/bin/env python3
"""
Analisi 3.1 — Statistiche Compagnie Aeree
Tecnologia: Spark SQL 3.5.8
"""
import os
import sys
import glob
import shutil
import time
from pathlib import Path
from pyspark.sql import SparkSession

CLUSTER_MODE   = os.environ.get("CLUSTER_MODE", "false").lower() == "true"
S3_OUTPUT_BASE = os.environ.get("S3_OUTPUT_BASE", "")

# ─── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH   = sys.argv[1] if len(sys.argv) > 1 else str(PROJECT_ROOT / "data" / "cleaned" / "flight_data_2024_cleaned.csv")

if CLUSTER_MODE and S3_OUTPUT_BASE:
    OUTPUT_PATH = f"{S3_OUTPUT_BASE}/analysis_1/spark_sql"
else:
    OUTPUT_PATH = str(PROJECT_ROOT / "results" / "analysis_1" / "spark_sql")
    os.makedirs(OUTPUT_PATH, exist_ok=True)

# ─── SparkSession ─────────────────────────────────────────────────────────────
_shuffle_parts = "200" if CLUSTER_MODE else "8"
builder = SparkSession.builder \
    .appName("Analysis_3.1_AirlineStats_SparkSQL") \
    .config("spark.sql.shuffle.partitions", _shuffle_parts)
if not CLUSTER_MODE:
    builder = builder.master("local[*]").config("spark.driver.memory", "4g")
spark = builder.getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"Spark versione: {spark.version}")
print(f"Input: {INPUT_PATH}")

# ─── Caricamento ──────────────────────────────────────────────────────────────
start = time.time()

df = spark.read.parquet(INPUT_PATH)
print(f"Righe caricate: {df.count():,}")

df.createOrReplaceTempView("flights")

# ─── Query con mesi attivi (allineata a Hive e MapReduce) ─────────────────────
query = """
    WITH monthly AS (
        SELECT
            op_unique_carrier                                       AS carrier,
            origin,
            month,
            COUNT(*)                                                AS num_flights,
            ROUND(MIN(arr_delay), 2)                                AS min_arr_delay,
            ROUND(MAX(arr_delay), 2)                                AS max_arr_delay,
            ROUND(AVG(arr_delay), 2)                                AS avg_arr_delay,
            ROUND(SUM(CAST(cancelled AS DOUBLE)) / COUNT(*), 4)    AS cancel_rate
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

results = spark.sql(query)

# ─── Salvataggio ──────────────────────────────────────────────────────────────
if CLUSTER_MODE:
    results.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", "|") \
        .csv(OUTPUT_PATH)
    print(f"Risultati salvati in: {OUTPUT_PATH}")
else:
    results.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", "|") \
        .csv(OUTPUT_PATH + "/raw")
    part_files = glob.glob(f"{OUTPUT_PATH}/raw/part-*.csv")
    if part_files and "cleaned" in INPUT_PATH:
        shutil.copy(part_files[0], f"{OUTPUT_PATH}/output.csv")
        print(f"Dataset completo rilevato. Risultati salvati in {OUTPUT_PATH}/output.csv")
    elif part_files:
        print(f"Dataset sample rilevato. Salto aggiornamento {OUTPUT_PATH}/output.csv")
    shutil.rmtree(f"{OUTPUT_PATH}/raw", ignore_errors=True)

elapsed = round(time.time() - start, 2)
print(f"\nTempo di esecuzione: {elapsed}s")
print(f"Risultato: {OUTPUT_PATH}/output.csv")

# ─── Prime 10 righe ───────────────────────────────────────────────────────────
print("\n=== Prime 10 righe ===")
results.show(10, truncate=False)

spark.stop()