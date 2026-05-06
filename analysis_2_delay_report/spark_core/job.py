#!/usr/bin/env python3
"""
Analisi 3.2 — Report Ritardi per Aeroporto e Periodo Temporale
Tecnologia: Spark Core 3.5.8 (RDD API) - UNIFICATO ORIZZONTALE
"""
import os, sys, time, glob, shutil
from pathlib import Path
from pyspark.sql import SparkSession

# ─── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH   = sys.argv[1] if len(sys.argv) > 1 else str(PROJECT_ROOT / "data" / "cleaned" / "flight_data_2024_cleaned.parquet")
OUTPUT_PATH  = str(PROJECT_ROOT / "results" / "analysis_2" / "spark_core")
os.makedirs(OUTPUT_PATH, exist_ok=True)

# ─── SparkSession ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Analysis_3.2_DelayReport_SparkCore") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

print(f"Spark versione: {sc.version}")
print(f"Input: {INPUT_PATH}")

start = time.time()

# ─── 1. Caricamento Parquet ───────────────────────────────────────────────────
df = spark.read.parquet(INPUT_PATH)
records = df.rdd.map(lambda r: (
    r.origin, 
    r.month, 
    r.dep_delay,
    r.arr_delay,
    r.carrier_delay or 0.0, 
    r.weather_delay or 0.0, 
    r.nas_delay or 0.0, 
    r.security_delay or 0.0, 
    r.late_aircraft_delay or 0.0
))
records.cache()

# ─── 2. Calcolo Fasce di Ritardo (BAND) ──────────────────────────────────────
def get_band(delay):
    if delay < 15: return 'low'
    if delay <= 60: return 'medium'
    return 'high'

bands_rdd = records.filter(lambda r: r[2] is not None) \
    .map(lambda r: ((r[0], r[1]), (get_band(r[2]), 1, r[2] or 0.0, r[3] or 0.0))) \
    .map(lambda x: ((x[0][0], x[0][1], x[1][0]), (x[1][1], x[1][2], x[1][3]))) \
    .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2])) \
    .map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1][0], round(x[1][1]/x[1][0], 2), round(x[1][2]/x[1][0], 2))))

# ─── 3. Calcolo Cause Pivotate (TOP 3) ───────────────────────────────────────
causes_avg = records.flatMap(lambda r: [
    ((r[0], r[1], "carrier"), (r[4], 1)) if r[4] > 0 else None,
    ((r[0], r[1], "weather"), (r[5], 1)) if r[5] > 0 else None,
    ((r[0], r[1], "nas"),     (r[6], 1)) if r[6] > 0 else None,
    ((r[0], r[1], "security"),(r[7], 1)) if r[7] > 0 else None,
    ((r[0], r[1], "late_aircraft"), (r[8], 1)) if r[8] > 0 else None
]).filter(lambda x: x is not None) \
  .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
  .map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1][0]/x[1][1])))

def pivot_causes(partition):
    res = []
    for key, values in partition:
        sorted_causes = sorted(list(values), key=lambda x: x[1], reverse=True)[:3]
        c1 = sorted_causes[0][0] if len(sorted_causes) > 0 else "none"
        c2 = sorted_causes[1][0] if len(sorted_causes) > 1 else "none"
        c3 = sorted_causes[2][0] if len(sorted_causes) > 2 else "none"
        res.append((key, (c1, c2, c3)))
    return res

causes_pivoted_rdd = causes_avg.groupByKey().mapValues(lambda x: pivot_causes_logic(x))

def pivot_causes_logic(values):
    sorted_causes = sorted(list(values), key=lambda x: x[1], reverse=True)[:3]
    c1 = sorted_causes[0][0] if len(sorted_causes) > 0 else "none"
    c2 = sorted_causes[1][0] if len(sorted_causes) > 1 else "none"
    c3 = sorted_causes[2][0] if len(sorted_causes) > 2 else "none"
    return (c1, c2, c3)

causes_pivoted_rdd = causes_avg.groupByKey().mapValues(pivot_causes_logic)

# ─── 4. Join e Salvataggio ───────────────────────────────────────────────────
final_rdd = bands_rdd.join(causes_pivoted_rdd) \
    .map(lambda x: (x[0][0], x[0][1], x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3], x[1][1][0], x[1][1][1], x[1][1][2]))

# Schema finale: origin, month, band, num, avg_dep, avg_arr, cause1, cause2, cause3
final_df = spark.createDataFrame(final_rdd, ["origin", "month", "delay_band", "num_flights", "avg_dep", "avg_arr", "top_cause_1", "top_cause_2", "top_cause_3"])

final_df.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", "|") \
    .csv(os.path.join(OUTPUT_PATH, "temp"))

parts = glob.glob(os.path.join(OUTPUT_PATH, "temp", "part-*.csv"))
if parts:
    shutil.copy(parts[0], os.path.join(OUTPUT_PATH, "output.csv"))
shutil.rmtree(os.path.join(OUTPUT_PATH, "temp"), ignore_errors=True)

elapsed = round(time.time() - start, 2)
print(f"\nTempo di esecuzione: {elapsed}s")
final_df.show(10, truncate=False)

spark.stop()