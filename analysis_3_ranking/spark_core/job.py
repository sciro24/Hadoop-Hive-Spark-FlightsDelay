#!/usr/bin/env python3
"""
Analisi 3.3 — Ranking coppie compagnia-aeroporto
Tecnologia: Spark Core 3.5.8 (RDD API)
"""
import os
import sys
import glob
import shutil
import time
from pathlib import Path
from pyspark import SparkContext, SparkConf


# ─── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH   = sys.argv[1] if len(sys.argv) > 1 else str(PROJECT_ROOT / "data" / "cleaned" / "flight_data_2024_cleaned.csv")
OUTPUT_PATH  = str(PROJECT_ROOT / "results" / "analysis_3" / "spark_core")
os.makedirs(OUTPUT_PATH, exist_ok=True)


# ─── Pulizia output precedente ────────────────────────────────────────────────
ranking_raw = os.path.join(OUTPUT_PATH, "ranking_raw")
if os.path.exists(ranking_raw):
    shutil.rmtree(ranking_raw)
    print(f"Rimossa directory precedente: {ranking_raw}")


# ─── SparkContext ─────────────────────────────────────────────────────────────
conf = SparkConf() \
    .setAppName("Analysis_3.3_Ranking_SparkCore") \
    .setMaster("local[*]") \
    .set("spark.driver.memory", "4g")

from pyspark.sql import SparkSession
spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

print(f"Spark versione: {sc.version}")
print(f"Input: {INPUT_PATH}")

start = time.time()


# ─── 1. Caricamento e parsing Parquet ─────────────────────────────────────────
df = spark.read.parquet(INPUT_PATH)
# Convertiamo in RDD di tuple: (origin, carrier, dep_delay, arr_delay, cancelled)
records = df.rdd.map(lambda r: (
    r.origin, r.op_unique_carrier, r.dep_delay, r.arr_delay, r.cancelled
))

records.cache()


# ─── 2. Statistiche per (origin, carrier) ────────────────────────────────────
carrier_stats = records \
    .map(lambda r: ((r[0], r[1]), (r[2], r[3], r[4], 1))) \
    .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2], a[3]+b[3]))


# ─── 3. Media globale dep_delay per aeroporto ────────────────────────────────
airport_avg = records \
    .map(lambda r: (r[0], (r[2], 1))) \
    .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
    .mapValues(lambda v: round(v[0] / v[1], 4))


# ─── 4. Join: carrier_stats con airport_avg ───────────────────────────────────
carrier_by_origin = carrier_stats \
    .map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))

joined = carrier_by_origin.join(airport_avg)


def compute_row(kv):
    origin, ((carrier, (dep_sum, arr_sum, canc_sum, count)), avg_airport) = kv
    avg_dep  = round(dep_sum  / count, 4)
    avg_arr  = round(arr_sum  / count, 4)
    cancel_r = round(canc_sum / count, 4)
    dep_diff = round(avg_dep - avg_airport, 4)
    return (origin, carrier, count, avg_dep, avg_arr, cancel_r, avg_airport, dep_diff)


rows = joined.map(compute_row)


# ─── 5. Rank per aeroporto ────────────────────────────────────────────────────
by_airport = rows \
    .groupBy(lambda r: r[0]) \
    .flatMap(lambda kv: [
        r + (rank + 1,)
        for rank, r in enumerate(sorted(kv[1], key=lambda x: x[3]))
    ])

result = by_airport.sortBy(lambda x: (x[0], x[8]))


# ─── 6. Salvataggio ───────────────────────────────────────────────────────────
out = result.map(lambda x:
    f"{x[0]}|{x[1]}|{x[2]}|{x[3]}|{x[4]}|{x[5]}|{x[6]}|{x[7]}|{x[8]}"
)
out.coalesce(1).saveAsTextFile(f"{OUTPUT_PATH}/ranking_raw")

parts = glob.glob(f"{OUTPUT_PATH}/ranking_raw/part-*")
if parts:
    with open(f"{OUTPUT_PATH}/output.csv", "w") as fout:
        fout.write("origin|carrier|num_flights|avg_dep|avg_arr|cancel_rate|avg_dep_airport|dep_diff|rank\n")
        with open(parts[0], "r") as fin:
            shutil.copyfileobj(fin, fout)
shutil.rmtree(f"{OUTPUT_PATH}/ranking_raw", ignore_errors=True)

elapsed = round(time.time() - start, 2)
print(f"\nTempo di esecuzione: {elapsed}s")
print(f"Risultati in: {OUTPUT_PATH}")


# ─── 7. Prime 10 righe ───────────────────────────────────────────────────────
print("\n=== Prime 10 righe ===")
print("origin|carrier|num_flights|avg_dep|avg_arr|cancel_rate|avg_dep_airport|dep_diff|rank")
for row in result.take(10):
    print("|".join(str(x) for x in row))


sc.stop()